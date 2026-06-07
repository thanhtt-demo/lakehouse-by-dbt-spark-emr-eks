# Spark Backends (de-team)

dbt-spark models run on one of two pluggable backends, chosen **per-model**. The backend only
changes *how* the Spark job is launched — the dbt project, `method: session`, and Iceberg/Glue
catalog config are identical either way.

| Mode | Where dbt + Spark driver run | Cost | Dagster integration |
|---|---|---|---|
| `eks_client` *(default)* | In-process in the **Dagster step pod**, which is the Spark **driver** (client deploy mode). Executors are plain pods on EKS. | EC2 (Karpenter) + EKS only — **no EMR uplift** | dbt runs in-process; results yielded directly. dbt logs land natively in the step's compute logs. No Pipes. |
| `emr_containers` | EMR on EKS Virtual Cluster. Driver/executors are EMR-managed pods running `spark_entrypoint/entrypoint.py`. | EC2 + EKS + **EMR uplift** (per vCPU/GB) | `PipesEMRContainersClient` + `PipesS3MessageReader`. |

## Toggling

Resolution order (highest priority first):

1. **Per-model** — `meta.spark_config.mode` in a model's `schema.yml`:
   ```yaml
   meta:
     spark_config:
       mode: emr_containers   # eks_client | emr_containers
   ```
2. **Project-wide** — `vars.spark_submit_mode` in `dbt_project.yml`:
   ```yaml
   vars:
     spark_submit_mode: eks_client
   ```
3. **Package default** — `eks_client`.

The mode is compiled into the dbt manifest, so a change takes effect after
`dagster-dbt project prepare-and-package` rebuilds the manifest (CI) and the Code Image redeploys.

## Why per-model matters for the driver

In `eks_client` mode there is **no separate driver pod** — the Dagster step pod *is* the driver.
So driver sizing (`driver_cpu` / `driver_memory`) is applied to the step pod via the
`dagster-k8s/config` op tag, and each model is its own `@dbt_assets(select=<model>)` op. With the
`k8s_job_executor`, that gives every model its own right-sized driver pod. Executor sizing
(`executor_*`) is applied to the Spark executor pods the driver launches.

## Files

| File | Role |
|---|---|
| `__init__.py` | Exports `run_eks_client`, `run_emr_containers`. |
| `eks_client.py` | Builds a client-mode `SparkSession` (master k8s, `driver.host`=POD_IP, executor conf) and runs `dbtRunner.invoke(["build", ...])` in-process; yields `MaterializeResult` / `AssetCheckResult`. |
| `emr_containers.py` | Submits via `PipesEMRContainersClient.run`; the EMR driver pod runs `entrypoint.py`. |
| `base.py` | Shared `run_results.json` / SQL parsing helpers (same result semantics as `entrypoint.py`). |

## Infrastructure dependencies (eks_client path)

| Concern | Provided by |
|---|---|
| Executor pod AWS creds (S3 + Glue) | Shared **de-team** IRSA role on the `dagster-user-deployments` SA — the same SA the driver step pod uses. Executors run in the `dagster` namespace as this SA (one role for driver + executors). |
| Driver → executor pod lifecycle | Role/RoleBinding `spark-driver` in the **dagster** namespace (`argocd/namespaces/spark-rbac.yaml`) bound to `dagster:dagster-user-deployments`. Executors run in the dagster namespace because Spark looks up the driver pod in `spark.kubernetes.namespace`. |
| Executors land on Spot NodePool | `spark_entrypoint/executor-pod-template.yaml` (toleration for `spark-role=executor:NoSchedule`), referenced via `SPARK_EXECUTOR_POD_TEMPLATE_FILE`. |
| Driver ↔ executor wiring (POD_IP/POD_NAME) | Downward API env injected by `SparkConfigManager.build_k8s_driver_op_tags`. |
| `k8s_job_executor` | `dagster-k8s` (installed in `Dockerfile.base`), set in `definitions.py`. |

Runtime env vars (set in `argocd/dagster/values.yaml`): `SPARK_K8S_NAMESPACE`,
`SPARK_EXECUTOR_SERVICE_ACCOUNT`, `SPARK_EXECUTOR_POD_TEMPLATE_FILE`, `SPARK_CODE_IMAGE_URI`,
`DBT_PROJECT_DIR`, and optional `SPARK_DRIVER_PORT` / `SPARK_BLOCKMANAGER_PORT`.

## Rollback

The `emr_containers` path is unchanged and fully retained. To revert a model (or the whole
project) to EMR, set the mode back — no infra teardown required. The EMR Virtual Cluster has no
standing cost when idle (you only pay the uplift while a job runs), so it is safe to keep as the
fallback.

## dbt parse performance (partial-parse cache)

Each model run starts a fresh dbt process (its own step pod), and dbt always parses the whole
project before applying `--select`. To avoid a full re-parse every time, both backends seed the
runtime `--target-path` with `partial_parse.msgpack` baked into the image:

- CI (`dagster-dbt project prepare-and-package`) generates `target/partial_parse.msgpack`; it is
  copied into the Code Image because `.dockerignore` intentionally keeps `dbt_project/target/`.
- At runtime, `seed_partial_parse()` (eks_client) / inline copy (entrypoint) places it in the
  tmp target before invoking dbt, so dbt does a fast *partial* parse.
- The cache is version-coupled: `dbt-core` is pinned to the **same exact version** in
  `Dockerfile.base` (`ARG DBT_VERSION`) and `.github/workflows/ci-cd.yml`. A mismatch makes dbt
  silently discard the cache and full-parse — correct, just slower.
