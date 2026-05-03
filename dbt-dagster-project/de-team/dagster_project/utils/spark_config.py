# ---------------------------------------------------------------------------------------------------------------------
# SPARK CONFIG MANAGER
# Manages merge logic between default Spark config and per-model config from dbt meta.
# Pure logic component — no external service dependencies.
# ---------------------------------------------------------------------------------------------------------------------

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

import dagster as dg


# ---------------------------------------------------------------------------------------------------------------------
# DEFAULT SPARK PROPERTIES (Iceberg + Glue Catalog)
# These are always included in every Spark job unless explicitly overridden.
# ---------------------------------------------------------------------------------------------------------------------

DEFAULT_SPARK_PROPERTIES: Dict[str, str] = {
    "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.warehouse": "s3://lakehouse-data-lake/warehouse/",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.hadoop.hive.metastore.client.factory.class": (
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    ),
}


# ---------------------------------------------------------------------------------------------------------------------
# DATA CLASSES
# ---------------------------------------------------------------------------------------------------------------------

@dataclass
class SparkResourceConfig:
    """Spark resource configuration for a single dbt model."""

    driver_cpu: str = "1"
    driver_memory: str = "2g"
    executor_cpu: str = "1"
    executor_memory: str = "4g"
    executor_instances: int = 2


@dataclass
class SparkJobConfig:
    """Complete configuration for a Spark job (resources + Spark properties)."""

    resources: SparkResourceConfig = field(default_factory=SparkResourceConfig)
    spark_properties: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------------------------------------------------
# RESOURCE CONFIG FIELD NAMES
# Used for merge logic — maps dbt meta keys to SparkResourceConfig field names.
# ---------------------------------------------------------------------------------------------------------------------

_RESOURCE_FIELDS = {"driver_cpu", "driver_memory", "executor_cpu", "executor_memory", "executor_instances"}


# ---------------------------------------------------------------------------------------------------------------------
# SPARK CONFIG MANAGER
# ---------------------------------------------------------------------------------------------------------------------

class SparkConfigManager(dg.ConfigurableResource):
    """Merge per-model Spark config with default config.

    Merge rules:
    - Model config takes priority over default config
    - Only fields present in model meta override defaults
    - Missing fields in model meta fall back to default values

    Extends ConfigurableResource so Dagster can inject it as a resource
    into @dbt_assets and other asset functions.
    """

    # Serialized default config — Pydantic-compatible fields
    default_driver_cpu: str = "1"
    default_driver_memory: str = "2g"
    default_executor_cpu: str = "1"
    default_executor_memory: str = "4g"
    default_executor_instances: int = 2
    default_spark_properties: Dict[str, str] = {}

    @property
    def default_config(self) -> SparkJobConfig:
        """Reconstruct SparkJobConfig from flat Pydantic fields."""
        return SparkJobConfig(
            resources=SparkResourceConfig(
                driver_cpu=self.default_driver_cpu,
                driver_memory=self.default_driver_memory,
                executor_cpu=self.default_executor_cpu,
                executor_memory=self.default_executor_memory,
                executor_instances=self.default_executor_instances,
            ),
            spark_properties={**self.default_spark_properties},
        )

    def merge_config(self, model_meta: Optional[dict]) -> SparkJobConfig:
        """Merge model meta spark_config with default.

        Args:
            model_meta: dict from dbt model meta.spark_config. Can be None or partial.

        Returns:
            SparkJobConfig with model values taking priority over defaults.
        """
        if not model_meta:
            return SparkJobConfig(
                resources=SparkResourceConfig(**asdict(self.default_config.resources)),
                spark_properties={**self.default_config.spark_properties},
            )

        # Merge resource fields — model meta overrides default
        default_resources = asdict(self.default_config.resources)
        merged_resources: dict = {}
        for field_name in _RESOURCE_FIELDS:
            if field_name in model_meta:
                value = model_meta[field_name]
                # Coerce executor_instances to int
                if field_name == "executor_instances":
                    value = int(value)
                merged_resources[field_name] = value
            else:
                merged_resources[field_name] = default_resources[field_name]

        # Merge spark_properties — model meta overrides default
        merged_properties = {**self.default_config.spark_properties}
        meta_properties = model_meta.get("spark_properties", {})
        if isinstance(meta_properties, dict):
            merged_properties.update(meta_properties)

        return SparkJobConfig(
            resources=SparkResourceConfig(**merged_resources),
            spark_properties=merged_properties,
        )

    def build_start_job_run_params(
        self,
        config: SparkJobConfig,
        virtual_cluster_id: str,
        execution_role_arn: str,
        image_uri: str,
        release_label: str = "emr-7.5.0-latest",
        run_id: str = "",
    ) -> dict:
        """Build start_job_run_params dict for PipesEMRContainersClient.run().

        Returns:
            Dict containing releaseLabel, virtualClusterId, executionRoleArn,
            jobDriver.sparkSubmitJobDriver with entryPoint and sparkSubmitParameters.
        """
        res = config.resources

        # Build --conf flags for sparkSubmitParameters
        conf_pairs: List[str] = [
            f"--conf spark.kubernetes.container.image={image_uri}",
            f"--conf spark.driver.cores={res.driver_cpu}",
            f"--conf spark.driver.memory={res.driver_memory}",
            f"--conf spark.executor.cores={res.executor_cpu}",
            f"--conf spark.executor.memory={res.executor_memory}",
            f"--conf spark.executor.instances={res.executor_instances}",
        ]

        # Append all spark_properties as --conf flags
        for key, value in config.spark_properties.items():
            conf_pairs.append(f"--conf {key}={value}")

        spark_submit_parameters = " ".join(conf_pairs)

        params: dict = {
            "releaseLabel": release_label,
            "virtualClusterId": virtual_cluster_id,
            "executionRoleArn": execution_role_arn,
            "jobDriver": {
                "sparkSubmitJobDriver": {
                    "entryPoint": "local:///app/entrypoint.py",
                    "sparkSubmitParameters": spark_submit_parameters,
                },
            },
        }

        if run_id:
            params["clientToken"] = run_id

        return params
