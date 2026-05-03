# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER DEFINITIONS — de-team code location
# Registers all assets (dbt + Python-only) and resources (PipesEMRContainersClient, SparkConfigManager).
# This is the entry point that Dagster loads for the de-team code location.
# ---------------------------------------------------------------------------------------------------------------------

import os

import dagster as dg

from .assets.dbt_assets import de_team_dbt_assets
from .assets.python_assets import python_only_assets
from .resources import create_pipes_emr_client
from .utils.spark_config import (
    DEFAULT_SPARK_PROPERTIES,
    SparkConfigManager,
)


# ---------------------------------------------------------------------------------------------------------------------
# DEFAULT SPARK CONFIGURATION
# Applied to all dbt models unless overridden by model-level meta.spark_config.
# ---------------------------------------------------------------------------------------------------------------------

spark_config_manager = SparkConfigManager(
    default_spark_properties={**DEFAULT_SPARK_PROPERTIES},
)


# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER DEFINITIONS
# ---------------------------------------------------------------------------------------------------------------------

defs = dg.Definitions(
    assets=[de_team_dbt_assets, *python_only_assets],
    resources={
        "pipes_emr_containers_client": create_pipes_emr_client(
            pipes_s3_bucket=os.getenv("PIPES_S3_BUCKET", "lakehouse-pipes"),
        ),
        "spark_config_manager": spark_config_manager,
    },
)
