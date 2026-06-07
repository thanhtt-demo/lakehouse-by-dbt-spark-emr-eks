# ---------------------------------------------------------------------------------------------------------------------
# DBT ASSETS — de-team (dbt-spark on EKS)
# Each dbt model maps 1:1 to a Dagster asset. To get a real per-model Spark driver, each model
# is its own @dbt_assets(select=<model>) op (NOT one multi-asset op). With the k8s_job_executor,
# every model runs in its own step pod, sized from the model's driver_cpu/driver_memory.
#
# Execution backend is resolved per-model from spark_config.mode (utils/spark_config.py):
#   - eks_client     : dbt runs in-process in the step pod, which acts as the Spark driver in
#                      client mode (master k8s). No EMR uplift. (default)
#   - emr_containers : submit to EMR on EKS Virtual Cluster via PipesEMRContainersClient.
#
# Toggle globally in dbt_project.yml `vars.spark_submit_mode`, or per-model in
# `meta.spark_config.mode`.
# ---------------------------------------------------------------------------------------------------------------------

import functools
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, List, Optional

from dagster_aws.pipes import PipesEMRContainersClient
from dagster_dbt import DagsterDbtTranslator, DbtProject, dbt_assets

from ..spark_backends import run_eks_client, run_emr_containers
from ..utils.spark_config import (
    DEFAULT_SPARK_PROPERTIES,
    SPARK_MODE_EKS_CLIENT,
    SparkConfigManager,
    read_project_default_mode,
)


# ---------------------------------------------------------------------------------------------------------------------
# DBT PROJECT SETUP
# Dev: generate manifest at runtime via prepare_if_dev()
# Prod: manifest precompiled in Code_Image via `dagster-dbt project prepare-and-package`
# ---------------------------------------------------------------------------------------------------------------------

_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_project").resolve()

dbt_project = DbtProject(
    project_dir=_PROJECT_DIR,
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


def _iter_model_nodes(manifest: dict):
    """Yield (model_name, node) for every dbt model in the manifest."""
    for _node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model":
            yield node.get("name"), node


def _model_spark_config_meta(node: dict) -> Optional[dict]:
    """Read meta.spark_config from a model node (supports node.meta and node.config.meta)."""
    meta = node.get("meta") or node.get("config", {}).get("meta", {}) or {}
    return meta.get("spark_config")


# ---------------------------------------------------------------------------------------------------------------------
# CUSTOM DBT TRANSLATOR
# Injects spark_config from dbt model meta into Dagster asset metadata.
# ---------------------------------------------------------------------------------------------------------------------

class SparkDbtTranslator(DagsterDbtTranslator):
    """Custom translator that:
    - Maps dbt model folder (staging/intermediate/marts) to Dagster group name
    - Exposes spark_config from dbt model meta as Dagster metadata
    """

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        meta = dbt_resource_props.get("meta", {})
        if meta.get("dagster_group"):
            return meta["dagster_group"]
        fqn = dbt_resource_props.get("fqn", [])
        if len(fqn) >= 2:
            return fqn[1]  # e.g. "staging", "intermediate", "marts"
        return "default"

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        meta = dbt_resource_props.get("meta", {})
        spark_config = meta.get("spark_config", {})
        base_metadata = super().get_metadata(dbt_resource_props)
        return {**base_metadata, "spark_config": spark_config}


# ---------------------------------------------------------------------------------------------------------------------
# PER-MODEL ASSET FACTORY
# Builds one @dbt_assets op per model. Config + mode are resolved at definition time so op_tags
# (driver pod size) are correct; the resolved config is captured in the closure and reused at
# runtime to dispatch to the right backend.
# ---------------------------------------------------------------------------------------------------------------------

# Definition-time config manager: same defaults + project mode as the runtime resource, so the
# driver-pod op_tags computed here match the config used at runtime.
_PROJECT_DEFAULT_MODE = read_project_default_mode(str(_PROJECT_DIR))
_codegen_config_manager = SparkConfigManager(
    default_spark_properties={**DEFAULT_SPARK_PROPERTIES},
    default_mode=_PROJECT_DEFAULT_MODE,
)
_translator = SparkDbtTranslator()


def _build_model_assets(model_name: str, config):
    """Create a single-model @dbt_assets op that dispatches to the resolved Spark backend."""
    op_tags = SparkConfigManager.build_k8s_driver_op_tags(config)

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        select=model_name,
        name=model_name,
        dagster_dbt_translator=_translator,
        op_tags=op_tags,
    )
    def _model_assets(
        context,
        pipes_emr_containers_client: PipesEMRContainersClient,
        spark_config_manager: SparkConfigManager,
    ):
        if config.mode == SPARK_MODE_EKS_CLIENT:
            yield from run_eks_client(
                context,
                model_name=model_name,
                config=config,
                spark_config_manager=spark_config_manager,
            )
        else:
            yield from run_emr_containers(
                context,
                model_name=model_name,
                config=config,
                spark_config_manager=spark_config_manager,
                pipes_emr_containers_client=pipes_emr_containers_client,
            )

    return _model_assets


def build_de_team_dbt_assets() -> List[Any]:
    """Build the list of per-model @dbt_assets ops for the de-team dbt project."""
    manifest = get_parsed_manifest()
    assets: List[Any] = []
    for model_name, node in _iter_model_nodes(manifest):
        if not model_name:
            continue
        config = _codegen_config_manager.merge_config(_model_spark_config_meta(node))
        assets.append(_build_model_assets(model_name, config))
    return assets


# All per-model dbt assets for the de-team code location.
de_team_dbt_assets = build_de_team_dbt_assets()
