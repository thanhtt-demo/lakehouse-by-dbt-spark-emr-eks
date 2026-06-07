# ---------------------------------------------------------------------------------------------------------------------
# EKS CLIENT-MODE BACKEND (mode == eks_client)
# Runs dbt in-process inside the Dagster step pod. That pod IS the Spark driver (client deploy
# mode) talking to the in-cluster Kubernetes API; executors are plain k8s pods Spark launches
# itself. No EMR control plane -> no EMR uplift, only EC2 (Karpenter) + EKS cost.
#
# Flow (mirrors spark_entrypoint/entrypoint.py, minus the Dagster Pipes layer):
#   1. Build a client-mode SparkSession (master k8s, driver.host = step pod IP, executor conf).
#   2. dbtRunner().invoke(["build", "--select", model]) — dbt-spark `method: session` binds to
#      the active SparkSession in this same process via getOrCreate().
#   3. Parse run_results.json, emit AssetCheckResult per test + MaterializeResult for the model.
#   4. Stop Spark.
#
# Because dbt runs in this process, all dbt stdout/stderr (compiled SQL, PASS/FAIL, timings)
# lands in the Dagster step's compute logs natively — no Pipes needed. Executor logs still live
# in the executor pods (collect via CloudWatch/fluent-bit if needed).
# ---------------------------------------------------------------------------------------------------------------------

from __future__ import annotations

import glob
import os
import socket
import sys
import tempfile

import dagster as dg

from ..utils.spark_config import SparkConfigManager, SparkJobConfig
from .base import (
    collect_test_check_results,
    parse_run_results,
    read_model_sql,
    seed_partial_parse,
    summarize_dbt_failure,
)


# Common SPARK_HOME locations in the EMR on EKS base image.
_SPARK_HOME_CANDIDATES = ("/usr/lib/spark", "/opt/spark")


def _ensure_pyspark_importable() -> None:
    """Make the Spark-bundled pyspark importable from this (non-spark-submit) process.

    The EMR on EKS image ships pyspark under $SPARK_HOME/python (+ a py4j zip), NOT as a pip
    package. spark-submit normally injects these onto PYTHONPATH; in eks_client mode we run dbt
    in-process (no spark-submit), so we replicate that here (findspark-style). Using the bundled
    pyspark guarantees it matches the image's Spark JVM + Iceberg jars.

    No-op if pyspark already imports (e.g. local dev with a pip-installed pyspark).
    """
    try:
        import pyspark  # noqa: F401
        return
    except ImportError:
        pass

    spark_home = os.environ.get("SPARK_HOME")
    candidates = [spark_home] if spark_home else list(_SPARK_HOME_CANDIDATES)
    for home in candidates:
        if not home:
            continue
        py_dir = os.path.join(home, "python")
        if not os.path.isdir(py_dir):
            continue
        # Ensure pyspark's JVM launch can find Spark (jars, spark-submit) at runtime.
        os.environ.setdefault("SPARK_HOME", home)
        if py_dir not in sys.path:
            sys.path.insert(0, py_dir)
        for py4j in glob.glob(os.path.join(py_dir, "lib", "py4j-*.zip")):
            if py4j not in sys.path:
                sys.path.insert(0, py4j)
        return


def _resolve_driver_host() -> str:
    """Driver host executors dial back to. Prefer POD_IP (downward API); fall back to the
    resolved hostname IP for local/dev runs outside k8s."""
    pod_ip = os.getenv("POD_IP")
    if pod_ip:
        return pod_ip
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"


def run_eks_client(
    context,
    *,
    model_name: str,
    config: SparkJobConfig,
    spark_config_manager: SparkConfigManager,
):
    """Run a dbt model in-process with a client-mode SparkSession on EKS, yielding results.

    Reads runtime wiring from environment (set in the Dagster Helm values):
      SPARK_CODE_IMAGE_URI    — executor pod image (same Code Image as this driver pod)
      SPARK_K8S_NAMESPACE     — namespace for executor pods (default: spark)
      SPARK_EXECUTOR_SERVICE_ACCOUNT — SA (with IRSA: S3 + Glue) executor pods run as
      SPARK_EXECUTOR_POD_TEMPLATE_FILE — optional executor pod template (toleration for the
                                spark-executors taint); empty = node selector only
      SPARK_DRIVER_PORT / SPARK_BLOCKMANAGER_PORT — optional fixed ports (default: ephemeral)
      DBT_PROJECT_DIR         — dbt project dir (default: /app/dbt_project)
    """
    _ensure_pyspark_importable()
    from pyspark.sql import SparkSession

    image_uri = os.getenv("SPARK_CODE_IMAGE_URI", "")
    namespace = os.getenv("SPARK_K8S_NAMESPACE", "spark")
    executor_service_account = os.getenv("SPARK_EXECUTOR_SERVICE_ACCOUNT", "spark")
    executor_pod_template_file = os.getenv("SPARK_EXECUTOR_POD_TEMPLATE_FILE", "")
    driver_port = int(os.getenv("SPARK_DRIVER_PORT", "0") or "0")
    block_manager_port = int(os.getenv("SPARK_BLOCKMANAGER_PORT", "0") or "0")
    project_dir = os.getenv("DBT_PROJECT_DIR", "/app/dbt_project")

    driver_host = _resolve_driver_host()
    driver_pod_name = os.getenv("POD_NAME", socket.gethostname())

    spark_conf = spark_config_manager.build_eks_client_spark_conf(
        config=config,
        image_uri=image_uri,
        driver_host=driver_host,
        driver_pod_name=driver_pod_name,
        namespace=namespace,
        executor_service_account=executor_service_account,
        driver_port=driver_port,
        block_manager_port=block_manager_port,
        executor_pod_template_file=executor_pod_template_file,
    )

    context.log.info(
        f"[eks_client] model={model_name} driver_host={driver_host} "
        f"executors={config.resources.executor_instances} "
        f"({config.resources.executor_cpu}cpu/{config.resources.executor_memory} each)"
    )

    spark = None
    try:
        builder = SparkSession.builder.appName(f"dbt-{model_name}")
        builder = builder.config(map=spark_conf)
        spark = builder.enableHiveSupport().getOrCreate()

        # dbt working dirs must be writable; keep them off the (possibly read-only) project dir.
        dbt_tmp = tempfile.mkdtemp(prefix="dbt-", dir="/tmp")
        target_path = os.path.join(dbt_tmp, "target")
        log_path = os.path.join(dbt_tmp, "logs")
        os.environ["DBT_TARGET_PATH"] = target_path
        os.environ["DBT_LOG_PATH"] = log_path

        # Seed the fresh target with the baked partial-parse cache so dbt does a fast partial
        # parse instead of a full re-parse of the whole project on every model run.
        if seed_partial_parse(project_dir, target_path):
            context.log.info("[eks_client] seeded partial_parse.msgpack for fast dbt parse")

        from dbt.cli.main import dbtRunner

        dbt_args = [
            "build",
            "--select", model_name,
            "--project-dir", project_dir,
            "--profiles-dir", project_dir,
            "--target-path", target_path,
            "--log-path", log_path,
        ]
        if os.getenv("DBT_DEBUG", "").lower() in ("1", "true", "yes"):
            dbt_args.append("--debug")

        result = dbtRunner().invoke(dbt_args)
        run_results = parse_run_results(os.path.join(target_path, "run_results.json"))

        # Emit one AssetCheckResult per dbt test.
        if run_results:
            for check in collect_test_check_results(run_results, target_path):
                yield dg.AssetCheckResult(
                    check_name=check["check_name"],
                    passed=check["passed"],
                    metadata=check["metadata"],
                )

        # Fail fast with the real dbt error surfaced into the Dagster log.
        if not result.success:
            detail = summarize_dbt_failure(
                run_results, getattr(result, "exception", None), model_name
            )
            context.log.error(detail)
            raise RuntimeError(detail)

        # Build materialization metadata (mirrors entrypoint.py).
        metadata: dict = {"model_name": model_name, "dbt_command": "build", "submit_mode": config.mode}
        if run_results:
            test_results = [
                r for r in run_results.get("results", [])
                if r.get("unique_id", "").startswith("test.")
            ]
            metadata["test_count"] = len(test_results)
            metadata["tests_passed"] = sum(
                1 for t in test_results if t.get("status") == "pass"
            )

        compiled_sql = read_model_sql(target_path, model_name, "compiled")
        run_sql = read_model_sql(target_path, model_name, "run")
        if compiled_sql:
            metadata["compiled_sql"] = dg.MetadataValue.md(f"```sql\n{compiled_sql}\n```")
        if run_sql:
            metadata["executed_sql"] = dg.MetadataValue.md(f"```sql\n{run_sql}\n```")

        yield dg.MaterializeResult(metadata=metadata)
    finally:
        if spark is not None:
            spark.stop()
