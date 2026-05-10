# ---------------------------------------------------------------------------------------------------------------------
# SPARK ENTRYPOINT SCRIPT
# Runs inside the Spark Driver Pod on EMR on EKS.
# Baked into Code_Image at /app/entrypoint.py.
#
# Architecture:
# - spark-submit launches this as a PySpark application → SparkSession is available
# - dbt-spark with method: session connects directly to the active SparkSession in-process
# - dbt build (= run + test) preserves full dbt features: incremental, tests, hooks, macros
# - Results are reported back to Dagster via PipesS3MessageWriter
# ---------------------------------------------------------------------------------------------------------------------

from __future__ import annotations

import json
import os
from typing import Optional

import boto3
from dagster_pipes import PipesS3MessageWriter, open_dagster_pipes
from pyspark.sql import SparkSession


def main() -> None:
    """Entrypoint for Spark job — runs dbt build and reports results via Dagster Pipes."""
    s3_client = boto3.client("s3")
    spark = None
    try:
        with open_dagster_pipes(
            message_writer=PipesS3MessageWriter(client=s3_client),
        ) as pipes:
            model_name = pipes.get_extra("model_name")
            dbt_command = pipes.get_extra("dbt_command")
            pipes.log.info(f"Running dbt {dbt_command} --select {model_name}")

            # 1. Get or create SparkSession (already available from spark-submit)
            spark = (
                SparkSession.builder
                .appName(f"dbt-{model_name}")
                .enableHiveSupport()
                .getOrCreate()
            )

            # 2. Run dbt via dbtRunner (Python API, same process as SparkSession)
            #    dbt-spark session method auto-discovers the active SparkSession.
            #    EMR on EKS Spark pods run with a read-only root filesystem, so
            #    dbt cannot write target/ or logs/ next to the baked-in project.
            #    Redirect both to /tmp (writable tmpfs) via --target-path and
            #    --log-path (also set env vars as a safety net for child processes).
            from dbt.cli.main import dbtRunner

            import tempfile
            dbt_tmp = tempfile.mkdtemp(prefix="dbt-", dir="/tmp")
            target_path = os.path.join(dbt_tmp, "target")
            log_path = os.path.join(dbt_tmp, "logs")
            os.environ["DBT_TARGET_PATH"] = target_path
            os.environ["DBT_LOG_PATH"] = log_path

            dbt_runner = dbtRunner()
            dbt_args = [
                dbt_command,
                "--select", model_name,
                "--project-dir", "/app/dbt_project",
                "--profiles-dir", "/app/dbt_project",
                "--target-path", target_path,
                "--log-path", log_path,
            ]
            # Toggle dbt debug logging (shows SQL statements + timings in dbt.log)
            # via env var so prod runs stay quiet unless explicitly requested.
            if os.getenv("DBT_DEBUG", "").lower() in ("1", "true", "yes"):
                dbt_args.append("--debug")

            result = dbt_runner.invoke(dbt_args)

            # 3. Parse run_results.json (from the writable target dir)
            run_results = _parse_run_results(os.path.join(target_path, "run_results.json"))

            # 4. Report test results as AssetCheckResult
            if run_results:
                _report_test_results(pipes, run_results)

            # 5. Fail fast if dbt failed — surface the actual dbt error so it shows
            #    up in Dagster logs (not just the generic "dbt build failed").
            if not result.success:
                error_lines: list[str] = []
                # dbt captures an exception on the runner result when the CLI failed
                # before emitting run_results (e.g. parse errors, connection errors).
                if getattr(result, "exception", None):
                    error_lines.append(f"dbtRunner exception: {result.exception!r}")
                # When the command actually ran models, each node result carries its
                # own status + message; surface every non-pass row.
                if run_results:
                    for r in run_results.get("results", []):
                        status = r.get("status", "")
                        if status in ("success", "pass"):
                            continue
                        uid = r.get("unique_id", "")
                        msg = r.get("message", "") or ""
                        error_lines.append(f"[{status}] {uid}: {msg}")
                detail = "; ".join(error_lines) if error_lines else "no detail from dbt"
                pipes.log.error(f"dbt {dbt_command} failed for {model_name}: {detail}")
                raise RuntimeError(
                    f"dbt {dbt_command} failed for model {model_name}: {detail}"
                )

            # 6. Report materialization with monitoring metadata
            region = os.getenv("AWS_REGION", "ap-southeast-1")
            virtual_cluster_id = os.getenv("EMR_VIRTUAL_CLUSTER_ID", "")

            metadata: dict = {
                "model_name": model_name,
                "dbt_command": dbt_command,
            }
            if virtual_cluster_id:
                metadata["emr_console_url"] = (
                    f"https://console.aws.amazon.com/emr/home?region={region}"
                    f"#/containers/virtual-clusters/{virtual_cluster_id}/jobs"
                )
            if run_results:
                test_results = [
                    r for r in run_results.get("results", [])
                    if r.get("unique_id", "").startswith("test.")
                ]
                metadata["test_count"] = len(test_results)
                metadata["tests_passed"] = sum(
                    1 for t in test_results if t.get("status") == "pass"
                )

            # Surface the compiled + executed SQL so it shows up directly in the
            # Dagster UI asset materialization metadata. dbt writes two variants:
            # - target/compiled/...  : Jinja-rendered SQL (the pure SELECT)
            # - target/run/...       : full statement Spark actually executes
            #                          (e.g. CREATE OR REPLACE TABLE ... AS SELECT)
            compiled_sql = _read_model_sql(target_path, model_name, "compiled")
            run_sql = _read_model_sql(target_path, model_name, "run")
            if compiled_sql:
                metadata["compiled_sql"] = compiled_sql
            if run_sql:
                metadata["executed_sql"] = run_sql
                pipes.log.info(f"Executed SQL for {model_name}:\n{run_sql}")

            pipes.report_asset_materialization(metadata=metadata)
            # pipes context manager flushes + closes the S3 message writer on exit.
    finally:
        # 7. Cleanup — stop Spark AFTER pipes is fully closed to avoid the
        #    "unexpected message received after closed" warning that happens
        #    when Spark driver prints shutdown logs into a pipes stream that
        #    the Dagster side has already marked as closed.
        if spark is not None:
            spark.stop()


# ---------------------------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------------------------------------------------

def _report_test_results(pipes, run_results: dict) -> None:
    """Report dbt test results as AssetCheckResult via Pipes.

    dbt test unique_id format: `test.{package}.{test_name}.{hash}` for generic
    tests, or `test.{package}.{test_name}` for singular tests. The Dagster
    check name must match the one registered by `@dbt_assets` (which uses the
    bare test_name, e.g. `not_null_stg_raw_orders_customer_id`) — NOT the
    trailing hash. Parse `parts[2]` so we always pick the name segment.
    """
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        if not unique_id.startswith("test."):
            continue

        parts = unique_id.split(".")
        if len(parts) < 3:
            continue
        test_name = parts[2]

        passed = result.get("status") == "pass"
        message = result.get("message", "") or ""

        pipes.report_asset_check(
            check_name=test_name,
            passed=passed,
            metadata={
                "test_unique_id": unique_id,
                "test_message": message,
                "severity": result.get("severity", "ERROR"),
            },
        )


def _parse_run_results(path: str) -> Optional[dict]:
    """Parse dbt run_results.json if it exists."""
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _read_model_sql(target_path: str, model_name: str, kind: str) -> Optional[str]:
    """Read the compiled or executed SQL for a dbt model.

    dbt writes artifacts under:
        {target_path}/{kind}/{project}/models/.../{model_name}.sql
    where `kind` is either `compiled` (Jinja-rendered SELECT) or `run`
    (the full DDL/DML Spark executes). We don't know the nested folder
    structure up front so we walk the tree once.
    """
    import os
    root = os.path.join(target_path, kind)
    if not os.path.isdir(root):
        return None
    target_filename = f"{model_name}.sql"
    for dirpath, _dirnames, filenames in os.walk(root):
        if target_filename in filenames:
            try:
                with open(os.path.join(dirpath, target_filename)) as f:
                    return f.read()
            except OSError:
                return None
    return None


if __name__ == "__main__":
    main()
