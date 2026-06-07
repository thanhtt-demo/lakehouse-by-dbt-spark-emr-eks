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
#
# Catalog strategy: wrap the built-in Spark session catalog (`spark_catalog`)
# with Iceberg's SparkSessionCatalog, backed by AWS Glue. With this layout,
# every unqualified `schema.table` reference in dbt (no `catalog.` prefix)
# resolves through a single catalog that handles both Iceberg and legacy
# Hive/Parquet tables. This matches how Athena registers tables in Glue,
# so Spark can read sources created by Athena (and vice versa) without any
# qualified-name changes in the dbt project.
#
# Alternative (requires prefixing every reference with `glue_catalog.`):
#   spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog
#   spark.sql.defaultCatalog       = glue_catalog
# ---------------------------------------------------------------------------------------------------------------------

DEFAULT_SPARK_PROPERTIES: Dict[str, str] = {
    # Enable Iceberg SQL extensions (MERGE INTO, time travel, rewrite_data_files).
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # Wrap Spark's default session catalog with Iceberg's SparkSessionCatalog.
    # Iceberg handles Iceberg tables; everything else falls back to the
    # underlying Hive-compatible metastore (AWS Glue via the client factory below).
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.spark_catalog.warehouse": "s3://lakehouse-at-scale-data-lake/warehouse/",
    "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    # AWS Glue as the Hive metastore backend for non-Iceberg fallback paths.
    "spark.hadoop.hive.metastore.client.factory.class": (
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    ),
}


# ---------------------------------------------------------------------------------------------------------------------
# SPARK SUBMIT MODES
# Two execution backends for dbt-spark models, toggled per-project (dbt_project.yml vars)
# or per-model (meta.spark_config.mode):
#
#   - emr_containers : submit to EMR on EKS Virtual Cluster via PipesEMRContainersClient
#                      (managed runtime, pay EMR uplift per vCPU/GB).
#   - eks_client     : run dbt in-process inside the Dagster step pod, with that pod acting
#                      as the Spark *driver* in client mode (master k8s://kubernetes.default.svc).
#                      Executors are plain k8s pods. No EMR uplift — only EC2 + EKS cost.
#
# Both share the same merged SparkJobConfig (resources + spark_properties); only the launch
# mechanism and the resulting Spark conf differ.
# ---------------------------------------------------------------------------------------------------------------------

SPARK_MODE_EMR_CONTAINERS = "emr_containers"
SPARK_MODE_EKS_CLIENT = "eks_client"
SPARK_SUBMIT_MODES = (SPARK_MODE_EMR_CONTAINERS, SPARK_MODE_EKS_CLIENT)
DEFAULT_SPARK_SUBMIT_MODE = SPARK_MODE_EKS_CLIENT

# dbt_project.yml var that sets the project-wide default submit mode.
SPARK_SUBMIT_MODE_VAR = "spark_submit_mode"


def read_project_default_mode(project_dir: str) -> str:
    """Read the project-wide default submit mode from dbt_project.yml `vars.spark_submit_mode`.

    This is the on/off toggle the user flips in dbt yaml. Falls back to DEFAULT_SPARK_SUBMIT_MODE
    if the file/var is missing or invalid, so code load never breaks on a typo.
    """
    import os

    try:
        import yaml  # dbt depends on PyYAML, always available in this image
    except ImportError:
        return DEFAULT_SPARK_SUBMIT_MODE

    path = os.path.join(project_dir, "dbt_project.yml")
    try:
        with open(path) as f:
            project = yaml.safe_load(f) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError):
        return DEFAULT_SPARK_SUBMIT_MODE

    value = (project.get("vars", {}) or {}).get(SPARK_SUBMIT_MODE_VAR)
    if isinstance(value, str) and value.strip().lower() in SPARK_SUBMIT_MODES:
        return value.strip().lower()
    return DEFAULT_SPARK_SUBMIT_MODE


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
    """Complete configuration for a Spark job (resources + Spark properties + submit mode)."""

    resources: SparkResourceConfig = field(default_factory=SparkResourceConfig)
    spark_properties: Dict[str, str] = field(default_factory=dict)
    # Execution backend for this model. Resolved from (in priority order):
    # model meta.spark_config.mode -> project default -> DEFAULT_SPARK_SUBMIT_MODE.
    mode: str = DEFAULT_SPARK_SUBMIT_MODE


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
    # Project-wide default submit mode. Overridable per-model via meta.spark_config.mode.
    # Wired from dbt_project.yml `vars.spark_submit_mode` at definition time.
    default_mode: str = DEFAULT_SPARK_SUBMIT_MODE

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
            mode=self._normalize_mode(self.default_mode),
        )

    @staticmethod
    def _normalize_mode(mode: Optional[str]) -> str:
        """Validate + normalize a submit mode, falling back to the package default.

        Unknown values fall back to DEFAULT_SPARK_SUBMIT_MODE rather than raising,
        so a typo in dbt yaml degrades gracefully instead of breaking code load.
        """
        if isinstance(mode, str):
            candidate = mode.strip().lower()
            if candidate in SPARK_SUBMIT_MODES:
                return candidate
        return DEFAULT_SPARK_SUBMIT_MODE

    def merge_config(self, model_meta: Optional[dict]) -> SparkJobConfig:
        """Merge model meta spark_config with default.

        Args:
            model_meta: dict from dbt model meta.spark_config. Can be None or partial.

        Returns:
            SparkJobConfig with model values taking priority over defaults.
            `mode` resolves from model meta.mode -> project default -> package default.
        """
        if not model_meta:
            return SparkJobConfig(
                resources=SparkResourceConfig(**asdict(self.default_config.resources)),
                spark_properties={**self.default_config.spark_properties},
                mode=self.default_config.mode,
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

        # Resolve mode — model meta overrides project default
        merged_mode = (
            self._normalize_mode(model_meta["mode"])
            if "mode" in model_meta
            else self.default_config.mode
        )

        return SparkJobConfig(
            resources=SparkResourceConfig(**merged_resources),
            spark_properties=merged_properties,
            mode=merged_mode,
        )

    def build_start_job_run_params(
        self,
        config: SparkJobConfig,
        virtual_cluster_id: str,
        execution_role_arn: str,
        image_uri: str,
        release_label: str = "emr-7.13.0-latest",
        run_id: str = "",
        s3_logs_uri: str = "",
        cloudwatch_log_group: str = "",
    ) -> dict:
        """Build start_job_run_params dict for PipesEMRContainersClient.run().

        Args:
            s3_logs_uri: s3://bucket[/prefix] — ship driver/executor logs to S3.
            cloudwatch_log_group: CloudWatch log group name — stream driver/executor logs.

        Returns:
            Dict containing releaseLabel, virtualClusterId, executionRoleArn,
            jobDriver.sparkSubmitJobDriver with entryPoint and sparkSubmitParameters,
            and optional configurationOverrides.monitoringConfiguration when log sinks
            are provided.
        """
        res = config.resources

        # Build --conf flags for sparkSubmitParameters.
        # The Iceberg Spark runtime JAR is bundled with the EMR on EKS base image at a
        # well-known path; we must add it to --jars so IcebergSparkSessionExtensions is
        # on the driver/executor classpath before Spark applies spark.sql.extensions.
        # https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/tutorial-iceberg.html
        iceberg_jar = "local:///usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"
        conf_pairs: List[str] = [
            f"--jars {iceberg_jar}",
            f"--conf spark.kubernetes.container.image={image_uri}",
            f"--conf spark.driver.cores={res.driver_cpu}",
            f"--conf spark.driver.memory={res.driver_memory}",
            f"--conf spark.executor.cores={res.executor_cpu}",
            f"--conf spark.executor.memory={res.executor_memory}",
            f"--conf spark.executor.instances={res.executor_instances}",
            # Disable Python stdio buffering in the Spark driver so dbt's log
            # lines reach the Pipes stdio forwarder as they happen, not in one
            # big flush at interpreter shutdown. Delay from ~minutes (buffered)
            # to ~10-20 s (Pipes S3 poll interval).
            "--conf spark.kubernetes.driverEnv.PYTHONUNBUFFERED=1",
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

        # Monitoring configuration — ship Spark driver/executor logs to S3 + CloudWatch.
        # Without this, failed jobs leave no trace once EMR cleans up driver/executor pods.
        monitoring: dict = {}
        if s3_logs_uri:
            monitoring["s3MonitoringConfiguration"] = {"logUri": s3_logs_uri}
        if cloudwatch_log_group:
            monitoring["cloudWatchMonitoringConfiguration"] = {
                "logGroupName": cloudwatch_log_group,
                "logStreamNamePrefix": run_id or "dbt-spark",
            }
        if monitoring:
            params["configurationOverrides"] = {"monitoringConfiguration": monitoring}

        if run_id:
            params["clientToken"] = run_id

        return params

    # -----------------------------------------------------------------------------------------------------------------
    # EKS CLIENT MODE BUILDERS
    # For mode == eks_client: dbt runs in-process inside the Dagster step pod, which acts as the
    # Spark *driver* (client deploy mode). Executors are plain k8s pods scheduled by Spark itself.
    # No EMR control plane involved — only EC2 (Karpenter) + EKS cost, dropping the EMR uplift.
    # -----------------------------------------------------------------------------------------------------------------

    def build_eks_client_spark_conf(
        self,
        config: SparkJobConfig,
        image_uri: str,
        driver_host: str,
        driver_pod_name: str = "",
        namespace: str = "spark",
        executor_service_account: str = "spark",
        driver_port: int = 0,
        block_manager_port: int = 0,
        executor_pod_template_file: str = "",
        executor_pod_name_prefix: str = "",
        model_label: str = "",
        extra_conf: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """Build the Spark conf map for an in-process client-mode SparkSession on EKS.

        The Dagster step pod is the driver, so we must tell executors how to reach it:
        - spark.driver.host       : the step pod IP (routable via VPC CNI inside the cluster)
        - spark.driver.bindAddress: 0.0.0.0 so the driver binds inside the pod
        - spark.kubernetes.driver.pod.name: the step pod name (so executors are owned/labelled)

        Args:
            config: merged SparkJobConfig (resources + spark_properties).
            image_uri: container image for executor pods (same Code Image as the driver).
            driver_host: step pod IP, injected via the downward API (POD_IP env var).
            driver_pod_name: step pod name (downward API metadata.name); links executors to driver.
            namespace: k8s namespace executor pods are created in.
            executor_service_account: k8s SA (with IRSA: S3 + Glue) the executor pods run as.
            driver_port: fixed driver RPC port (0 = let Spark choose; fix it when a
                NetworkPolicy / headless Service requires a known port).
            block_manager_port: fixed block manager port (0 = ephemeral).
            executor_pod_template_file: optional path/URI to an executor pod template. Spark on
                k8s cannot express tolerations via plain conf, so to land executors on the tainted
                spark-executors NodePool (spark-role=executor:NoSchedule) a pod template carrying
                the matching toleration is required. Empty = rely on node selector only.
            extra_conf: optional overrides merged last (highest priority).

        Returns:
            Flat str->str dict suitable for SparkSession.builder.config(map=...).
        """
        res = config.resources
        conf: Dict[str, str] = {
            # --- Cluster wiring ---
            "spark.master": "k8s://https://kubernetes.default.svc",
            "spark.submit.deployMode": "client",
            "spark.kubernetes.namespace": namespace,
            # Executors run as the IRSA-annotated SA so they get S3 + Glue creds.
            # In client mode the driver is the step pod (uses its own mounted SA token),
            # so only the executor SA needs to be pinned here.
            "spark.kubernetes.authenticate.executor.serviceAccountName": executor_service_account,
            "spark.kubernetes.container.image": image_uri,
            # --- Driver reachability (client mode: driver lives in the step pod) ---
            "spark.driver.host": driver_host,
            "spark.driver.bindAddress": "0.0.0.0",
            # --- Resources ---
            "spark.driver.cores": str(res.driver_cpu),
            "spark.driver.memory": res.driver_memory,
            "spark.executor.cores": str(res.executor_cpu),
            "spark.executor.memory": res.executor_memory,
            "spark.executor.instances": str(res.executor_instances),
            # Land executor pods on the Karpenter spark-executors NodePool (Spot).
            "spark.kubernetes.node.selector.spark-role": "executor",
        }
        if executor_pod_template_file:
            # Pod template supplies the toleration for the spark-executors taint
            # (and any other executor-pod customization Spark conf can't express).
            conf["spark.kubernetes.executor.podTemplateFile"] = executor_pod_template_file
            # Spark must know which container in the template is the executor container to
            # merge its generated spec (image/resources/env) into. Must match the container
            # name in executor-pod-template.yaml; otherwise Spark errors with
            # "Container name is required when pod template is present".
            conf["spark.kubernetes.executor.podTemplateContainerName"] = "spark-kubernetes-executor"
        # Readable executor pod names (e.g. dbt-stg-raw-orders-<id>-exec-1) + a model label so
        # `kubectl get pods -l dbt-model=<model>` works. NOTE: do NOT set a custom `spark-role`
        # executor label — Spark reserves it and adds spark-role=executor itself (setting it via
        # spark.kubernetes.executor.label.* throws "reserved for Spark" and aborts the context).
        if executor_pod_name_prefix:
            conf["spark.kubernetes.executor.podNamePrefix"] = executor_pod_name_prefix
        if model_label:
            conf["spark.kubernetes.executor.label.dbt-model"] = model_label
        if driver_pod_name:
            conf["spark.kubernetes.driver.pod.name"] = driver_pod_name
        if driver_port:
            conf["spark.driver.port"] = str(driver_port)
        if block_manager_port:
            conf["spark.blockManager.port"] = str(block_manager_port)

        # Iceberg + Glue catalog properties (and any per-model spark_properties).
        conf.update(config.spark_properties)

        if extra_conf:
            conf.update(extra_conf)

        return conf

    @staticmethod
    def build_k8s_driver_op_tags(config: SparkJobConfig, model_name: str = "") -> dict:
        """Build Dagster `dagster-k8s/config` op_tags so the step pod (= Spark driver in
        client mode) is sized from the model's driver_cpu / driver_memory.

        This is the mechanism by which "the Dagster run uses the driver config from dbt yaml
        as the driver pod config": k8s_job_executor reads these tags when launching the
        per-model step pod.

        Memory is passed through verbatim (e.g. "4g" -> "4G" for k8s). CPU maps to a
        millicore-friendly string. Returns an empty dict for non-eks_client modes so the
        EMR path keeps a small fixed step pod (the real driver is the EMR pod).
        """
        if config.mode != SPARK_MODE_EKS_CLIENT:
            return {}

        res = config.resources
        # k8s memory uses Mi/Gi suffixes; Spark uses m/g. Normalize "4g" -> "4Gi", "512m" -> "512Mi".
        mem = res.driver_memory.strip().lower()
        if mem.endswith("g"):
            k8s_mem = f"{mem[:-1]}Gi"
        elif mem.endswith("m"):
            k8s_mem = f"{mem[:-1]}Mi"
        else:
            k8s_mem = mem

        # Labels so the step pod (= Spark driver) is easy to find:
        #   kubectl get pods -n dagster -l spark-role=driver
        #   kubectl get pods -n dagster -l dbt-model=<model>
        labels = {"spark-role": "driver"}
        if model_name:
            labels["dbt-model"] = model_name

        return {
            "dagster-k8s/config": {
                "container_config": {
                    "resources": {
                        "requests": {"cpu": f"{res.driver_cpu}", "memory": k8s_mem},
                        # Must set BOTH cpu and memory limits. The Dagster base job config /
                        # namespace default caps cpu at 500m; without an explicit cpu limit here
                        # the driver's cpu request (e.g. "1") would exceed that default limit and
                        # k8s rejects the Job (422 "request must be <= cpu limit").
                        "limits": {"cpu": f"{res.driver_cpu}", "memory": k8s_mem},
                    },
                    # Inject the step pod's own IP + name via the downward API. The eks_client
                    # backend reads POD_IP for spark.driver.host (so executors can dial back to
                    # this driver) and POD_NAME for spark.kubernetes.driver.pod.name.
                    "env": [
                        {
                            "name": "POD_IP",
                            "value_from": {"field_ref": {"field_path": "status.podIP"}},
                        },
                        {
                            "name": "POD_NAME",
                            "value_from": {"field_ref": {"field_path": "metadata.name"}},
                        },
                    ],
                },
                "pod_template_spec_metadata": {"labels": labels},
            },
        }
