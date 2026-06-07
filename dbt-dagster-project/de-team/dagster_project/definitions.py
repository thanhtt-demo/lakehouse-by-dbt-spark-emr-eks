# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER DEFINITIONS — de-team code location
# Registers all assets (dbt + Python-only) and resources.
# Each dbt model is its own @dbt_assets op; with the k8s_job_executor every model runs in its
# own step pod (= Spark driver in eks_client mode), sized from the model's driver config.
# ---------------------------------------------------------------------------------------------------------------------

import os

import dagster as dg
from dagster_k8s import k8s_job_executor

from .assets.dbt_assets import de_team_dbt_assets
from .assets.python_assets import python_only_assets
from .resources import create_pipes_emr_client
from .utils.spark_config import (
    DEFAULT_SPARK_PROPERTIES,
    SparkConfigManager,
    read_project_default_mode,
)


# ---------------------------------------------------------------------------------------------------------------------
# DEFAULT SPARK CONFIGURATION
# Applied to all dbt models unless overridden by model-level meta.spark_config.
# default_mode comes from dbt_project.yml `vars.spark_submit_mode` (the on/off toggle).
# ---------------------------------------------------------------------------------------------------------------------

_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/app/dbt_project")

spark_config_manager = SparkConfigManager(
    default_spark_properties={**DEFAULT_SPARK_PROPERTIES},
    default_mode=read_project_default_mode(_PROJECT_DIR),
)


# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER DEFINITIONS
# k8s_job_executor: one step pod per op. For eks_client models the step pod is the Spark driver.
# ---------------------------------------------------------------------------------------------------------------------

defs = dg.Definitions(
    assets=[*de_team_dbt_assets, *python_only_assets],
    executor=k8s_job_executor,
    resources={
        "pipes_emr_containers_client": create_pipes_emr_client(
            pipes_s3_bucket=os.getenv("PIPES_S3_BUCKET", "lakehouse-pipes"),
        ),
        "spark_config_manager": spark_config_manager,
    },
)
