# ---------------------------------------------------------------------------------------------------------------------
# DBT ASSETS — de-team (dbt-spark on EMR on EKS)
# Each dbt model maps 1:1 to a Dagster asset. Execution is delegated to Spark via EMR on EKS
# using PipesEMRContainersClient. The Spark pod runs `dbt build --select model_name` to preserve
# full dbt features (incremental models, tests, hooks, macros).
# ---------------------------------------------------------------------------------------------------------------------

import functools
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from dagster import EnvVar
from dagster_aws.pipes import PipesEMRContainersClient
from dagster_dbt import DagsterDbtTranslator, DbtProject, dbt_assets

from ..utils.spark_config import SparkConfigManager


# ---------------------------------------------------------------------------------------------------------------------
# DBT PROJECT SETUP
# Dev: generate manifest at runtime via prepare_if_dev()
# Prod: manifest precompiled in Code_Image via `dagster-dbt project prepare-and-package`
# ---------------------------------------------------------------------------------------------------------------------

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "dbt_project").resolve(),
    packaged_project_dir=(
        Path("/opt/dagster/app/dbt-project")
        if os.getenv("DAGSTER_ENV") == "prod"
        else None
    ),
)
if os.getenv("DAGSTER_ENV") != "prod":
    dbt_project.prepare_if_dev()


# ---------------------------------------------------------------------------------------------------------------------
# MANIFEST HELPERS
# ---------------------------------------------------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def get_parsed_manifest() -> dict:
    """Cache parsed manifest to avoid re-parsing on every asset materialization."""
    with open(dbt_project.manifest_path) as f:
        return json.load(f)


def _find_model_in_manifest(model_name: str) -> Optional[dict]:
    """Find a model node in the dbt manifest by name."""
    manifest = get_parsed_manifest()
    for _node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model" and node.get("name") == model_name:
            return node
    return None


# ---------------------------------------------------------------------------------------------------------------------
# CUSTOM DBT TRANSLATOR
# Injects spark_config from dbt model meta into Dagster asset metadata.
# ---------------------------------------------------------------------------------------------------------------------

class SparkDbtTranslator(DagsterDbtTranslator):
    """Custom translator that exposes spark_config from dbt model meta as Dagster metadata."""

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        meta = dbt_resource_props.get("meta", {})
        spark_config = meta.get("spark_config", {})
        base_metadata = super().get_metadata(dbt_resource_props)
        return {**base_metadata, "spark_config": spark_config}


# ---------------------------------------------------------------------------------------------------------------------
# DBT ASSETS DEFINITION
# Each dbt model becomes a Dagster asset. On materialize, a Spark job is submitted to EMR on EKS.
# The Spark pod runs `dbt build --select model_name` and reports results back via Dagster Pipes.
# ---------------------------------------------------------------------------------------------------------------------

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=SparkDbtTranslator(),
)
def de_team_dbt_assets(
    context,
    pipes_emr_containers_client: PipesEMRContainersClient,
    spark_config_manager: SparkConfigManager,
):
    """dbt assets for de-team — each model runs on Spark via EMR on EKS.

    Execution flow:
    1. Read manifest to identify which models to materialize
    2. Read spark_config from dbt model meta, merge with defaults
    3. Submit Spark job via PipesEMRContainersClient
    4. Spark pod runs `dbt build` (run + test)
    5. entrypoint.py reports materialization + test results via Pipes
    """
    # Read runtime config from environment
    virtual_cluster_id = os.getenv("EMR_VIRTUAL_CLUSTER_ID", "")
    execution_role_arn = os.getenv("EMR_EXECUTION_ROLE_ARN", "")
    # SPARK_CODE_IMAGE_URI carries the full ECR URI including tag (e.g. `...repo:<sha>`).
    # CI/CD updates this in lock-step with the Dagster user-deployment `image.tag` so the
    # Spark driver/executor pods always pull the same image version as the Dagster run pod.
    code_image_uri = os.getenv("SPARK_CODE_IMAGE_URI", "")

    for asset_key in context.selected_asset_keys:
        model_name = asset_key.path[-1]

        # Find model in manifest to read spark_config from meta
        model_node = _find_model_in_manifest(model_name)
        model_meta = model_node.get("meta", {}).get("spark_config") if model_node else None

        # Merge spark config (model meta takes priority over default)
        config = spark_config_manager.merge_config(model_meta)
        params = spark_config_manager.build_start_job_run_params(
            config=config,
            virtual_cluster_id=virtual_cluster_id,
            execution_role_arn=execution_role_arn,
            image_uri=code_image_uri,
            run_id=context.run_id,
        )

        # Submit Spark job — entrypoint.py runs `dbt build --select model_name`
        invocation = pipes_emr_containers_client.run(
            context=context,
            start_job_run_params=params,
            extras={
                "model_name": model_name,
                "dbt_command": "build",
            },
        )

        # Yield all events (MaterializeResult + AssetCheckResult) from Pipes
        yield from invocation.get_results()
