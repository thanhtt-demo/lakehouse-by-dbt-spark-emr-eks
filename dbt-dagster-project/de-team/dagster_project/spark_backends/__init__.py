# ---------------------------------------------------------------------------------------------------------------------
# SPARK BACKENDS — de-team
# Pluggable execution backends for dbt-spark models. The active backend is resolved per-model
# from SparkJobConfig.mode (see utils/spark_config.py):
#
#   - emr_containers : submit to EMR on EKS Virtual Cluster via PipesEMRContainersClient.
#                      Managed runtime; pays the EMR uplift per vCPU/GB.  (run_emr_containers)
#   - eks_client     : run dbt in-process in the Dagster step pod, which acts as the Spark
#                      driver in client mode against the in-cluster k8s API. No EMR uplift.
#                      (run_eks_client)
#
# Both are plain generator functions: run_<backend>(context, ...) -> yields Dagster events
# (MaterializeResult / AssetCheckResult). dbt_assets.py picks one based on config.mode.
# ---------------------------------------------------------------------------------------------------------------------

from .eks_client import run_eks_client
from .emr_containers import run_emr_containers

__all__ = ["run_eks_client", "run_emr_containers"]
