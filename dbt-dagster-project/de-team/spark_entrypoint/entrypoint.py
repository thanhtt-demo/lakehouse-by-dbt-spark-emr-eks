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
        #    dbt-spark session method auto-discovers the active SparkSession
        from dbt.cli.main import dbtRunner

        dbt_runner = dbtRunner()
        dbt_args = [
            dbt_command,
            "--select", model_name,
            "--project-dir", "/app/dbt_project",
            "--profiles-dir", "/app/dbt_project",
        ]

        result = dbt_runner.invoke(dbt_args)

        # 3. Parse run_results.json
        run_results = _parse_run_results("/app/dbt_project/target/run_results.json")

        # 4. Report test results as AssetCheckResult
        if run_results:
            _report_test_results(pipes, run_results)

        # 5. Fail fast if dbt failed
        if not result.success:
            pipes.log.error(f"dbt {dbt_command} failed for model {model_name}")
            raise RuntimeError(f"dbt {dbt_command} failed for model {model_name}")

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

        pipes.report_asset_materialization(metadata=metadata)

        # 7. Cleanup
        spark.stop()


# ---------------------------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------------------------------------------------

def _report_test_results(pipes, run_results: dict) -> None:
    """Report dbt test results as AssetCheckResult via Pipes."""
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        if not unique_id.startswith("test."):
            continue

        test_name = result.get("name", unique_id.split(".")[-1])
        passed = result.get("status") == "pass"
        message = result.get("message", "")

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


if __name__ == "__main__":
    main()
