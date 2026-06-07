# ---------------------------------------------------------------------------------------------------------------------
# EMR ON EKS BACKEND (mode == emr_containers)
# Submits one Spark job per dbt model to the EMR on EKS Virtual Cluster via
# PipesEMRContainersClient. The Spark driver pod runs spark_entrypoint/entrypoint.py, which
# executes `dbt build --select <model>` against an in-process SparkSession and reports results
# back through Dagster Pipes (PipesS3MessageWriter -> PipesS3MessageReader).
#
# This is the original execution path, factored out unchanged so it can be toggled per-model.
# Pays the EMR uplift; keep as the managed-runtime fallback.
# ---------------------------------------------------------------------------------------------------------------------

from __future__ import annotations

import os

from ..utils.spark_config import SparkConfigManager, SparkJobConfig


def run_emr_containers(
    context,
    *,
    model_name: str,
    config: SparkJobConfig,
    spark_config_manager: SparkConfigManager,
    pipes_emr_containers_client,
):
    """Submit a dbt model to EMR on EKS and yield Dagster results via Pipes.

    Reads EMR runtime wiring from environment (set in the Dagster Helm values):
      EMR_VIRTUAL_CLUSTER_ID, EMR_EXECUTION_ROLE_ARN, SPARK_CODE_IMAGE_URI,
      SPARK_S3_LOGS_URI, SPARK_CLOUDWATCH_LOG_GROUP.
    """
    virtual_cluster_id = os.getenv("EMR_VIRTUAL_CLUSTER_ID", "")
    execution_role_arn = os.getenv("EMR_EXECUTION_ROLE_ARN", "")
    # SPARK_CODE_IMAGE_URI carries the full ECR URI including tag; CI/CD keeps it in lock-step
    # with the Dagster user-deployment image.tag so driver/executor pods match the run pod.
    code_image_uri = os.getenv("SPARK_CODE_IMAGE_URI", "")
    s3_logs_uri = os.getenv("SPARK_S3_LOGS_URI", "")
    cloudwatch_log_group = os.getenv("SPARK_CLOUDWATCH_LOG_GROUP", "")

    params = spark_config_manager.build_start_job_run_params(
        config=config,
        virtual_cluster_id=virtual_cluster_id,
        execution_role_arn=execution_role_arn,
        image_uri=code_image_uri,
        run_id=context.run_id,
        s3_logs_uri=s3_logs_uri,
        cloudwatch_log_group=cloudwatch_log_group,
    )

    invocation = pipes_emr_containers_client.run(
        context=context,
        start_job_run_params=params,
        extras={
            "model_name": model_name,
            "dbt_command": "build",
        },
    )
    yield from invocation.get_results()
