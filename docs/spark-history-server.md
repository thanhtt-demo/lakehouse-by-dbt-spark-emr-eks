# Spark History Server (Spark UI)

A single standalone Spark History Server (SHS) serves the Spark UI for **finished and failed**
jobs by replaying Spark **event logs** from S3. It works the same for both execution backends
(`eks_client` and `emr_containers`).

> **Scope note:** the SHS is for *completed* jobs, not live monitoring. Spark event logs on S3
> only become readable when a log chunk is closed (at the rollover size, or on job stop), so a
> short running job does **not** show live progress here — see
> [Viewing a *running* job](#2-viewing-a-running-job) below.

## How it works

```
 Spark driver (step pod / EMR pod)                Spark History Server (spark ns)
 spark.eventLog.dir = s3://.../spark-events/  ──▶  s3://.../spark-events/  ──▶  Spark UI :18080
 rolling event logs, finalized on stop/rollover    polls every 15s (update.interval)
```

- **Writers** — every job sets (in `dagster_project/utils/spark_config.py` → `DEFAULT_SPARK_PROPERTIES`):
  - `spark.eventLog.enabled=true`
  - `spark.eventLog.dir=s3://lakehouse-at-scale-spark-logs/spark-events/`
  - `spark.eventLog.rolling.enabled=true`, `spark.eventLog.rolling.maxFileSize=128m`
- **Reader** — the SHS (`argocd/spark-history-server`) sets
  `spark.history.fs.logDirectory` to the same prefix and polls every `15s`.

## The two concerns this addresses

### 1. Job failed mid-way → missing Spark UI

Both backends call `spark.stop()` in a `finally` block (`spark_backends/eks_client.py` and
`spark_entrypoint/entrypoint.py`). So on a *normal* dbt failure — query error, failed test,
raised exception — Spark shuts down gracefully and the event log is **flushed and closed on S3**,
giving the History Server a complete, replayable log.

The only lossy case is a **hard kill** of the driver (OOMKilled, node evicted) before it can
flush. Rolling event logs bound that loss to the single currently-open chunk: every rolled chunk
is already durable on S3, and `spark.history.fs.inProgressOptimization.enabled=true` lets the SHS
render whatever was flushed. To debug a hard-killed driver, also use the driver/executor stdout
logs (CloudWatch `/emr-on-eks/lakehouse-at-scale` and `s3://.../emr-on-eks/` for the EMR path;
Dagster compute logs for the eks_client path).

### 2. Viewing a running job

**The SHS does not give a live view of a short running job.** Spark event logs are only durable
and readable on S3 once a log *chunk* is closed, which happens when:

1. the open chunk reaches `spark.eventLog.rolling.maxFileSize` (128m) — then it rolls and uploads, or
2. the job stops (`SparkContext.stop()`) — the open chunk is finalized.

On S3 (EMRFS/S3A) `flush()`/`hflush()` are effectively no-ops for durability, and Spark has **no
time-based flush** for event logs — rollover is size-based only. A typical dbt model produces an
event log far smaller than 128m, so nothing readable lands on S3 until the job ends. After it
ends, the SHS picks it up within `spark.history.fs.update.interval` (~15s). Lowering `maxFileSize`
(Spark minimum 10m) only helps jobs that emit very large logs; it is not a reliable live view.

**To watch a job while it runs, use the driver's live Spark UI on port 4040** (real-time, not
S3-backed):

```bash
# eks_client mode — driver is the Dagster step pod (namespace dagster)
kubectl -n dagster get pods -l spark-role=driver        # also: -l dbt-model=<model>
kubectl -n dagster port-forward pod/<driver-pod> 4040:4040
# open http://localhost:4040

# emr_containers mode — driver is an EMR driver pod (namespace spark)
kubectl -n spark get pods
kubectl -n spark port-forward pod/<driver-pod> 4040:4040
```

Division of responsibility:

| Goal | Use |
|---|---|
| Inspect a **finished / failed** job | Spark History Server (this doc), ~15s after the job ends |
| Watch a **running** job in real time | the driver's port-4040 UI via `kubectl port-forward` |

A first-class running-job UI (a Service/Ingress in front of the driver 4040 ports, or surfacing
the link in Dagster) is **not implemented yet** — port-forward is the current path.

## DataFlint (richer UI for live + completed jobs)

[DataFlint](https://github.com/dataflint/spark) is a Spark plugin that adds a modern,
real-time tab to the Spark UI. It is integrated here so the same plugin enhances **both** views:

- **Driver UI (:4040) while a job runs** — DataFlint gives a live query/cluster view, performance
  heat map, and alerts. This is the practical answer to "watch a running job", far nicer than the
  stock 4040 tabs (and not subject to the S3 event-log delay).
- **History Server (:18080) for finished jobs** — DataFlint also enriches completed runs replayed
  from S3 event logs.

How it is wired (no `--packages` download at runtime):

| Where | Mechanism |
|---|---|
| Plugin JAR | Baked into `$SPARK_HOME/jars` in `de-team/Dockerfile.base` (`io.dataflint:spark_2.12`). On the classpath of the job driver/executors and of the History Server (which runs the **base image** directly). |
| Job driver/executors | `spark.plugins=io.dataflint.spark.SparkDataflintPlugin` + `spark.dataflint.iceberg.autoCatalogDiscovery=true` in `DEFAULT_SPARK_PROPERTIES`. |
| History Server | Auto-discovered from the JAR's `META-INF` SPI — **no extra config**; the DataFlint tab appears when a run is loaded from event logs. |

Access is the same as the plain Spark UI — open the driver `:4040` (running) or the History
Server `:18080` (finished) and pick the **DataFlint** tab.

> **Rollout order:** rebuild `Dockerfile.base` (which bakes the JAR) **before** shipping the
> `spark.plugins` property, otherwise jobs fail at startup with `ClassNotFoundException` for
> `io.dataflint.spark.SparkDataflintPlugin`. The base→code image chain keeps them consistent as
> long as the base image is rebuilt first. Bump the version via the `DATAFLINT_VERSION` build arg.

## S3 access for the daemon (important)

The EMR image accesses S3 through **EMRFS** (`com.amazon.ws.emr.hadoop.fs.EmrFileSystem`, AWS
SDK v1), wired onto the *driver* JVM via `spark.driver.extraClassPath` in
`/etc/spark/conf/spark-defaults.conf`. That mechanism only applies to driver/executor JVMs
launched by `spark-submit` — it does **not** reach the HistoryServer daemon launched directly by
`spark-class`, which builds its classpath from the Spark jars + `SPARK_DIST_CLASSPATH` only.

Two things are therefore needed (both in `templates/deployment.yaml`):

1. **Add the EMR S3 jars to `SPARK_DIST_CLASSPATH`** before launching the daemon — EMRFS +
   AWS SDK v1 from `/usr/share/aws/emr/emrfs/{conf,lib,auxlib}` and `/usr/share/aws/aws-java-sdk/*`
   (plus `hadoop classpath`). Without them: `ClassNotFoundException: ...S3AFileSystem`.
2. **Force `fs.s3.impl=EmrFileSystem`** via `SPARK_HISTORY_OPTS`
   (`-Dspark.hadoop.fs.s3.impl=com.amazon.ws.emr.hadoop.fs.EmrFileSystem`). The default
   open-source S3A connector in EMR's Hadoop needs the AWS SDK **v2** bundle, which the EMR image
   does **not** ship (it ships SDK v1 for EMRFS). Leaving the default S3A fails with
   `NoClassDefFoundError: software.amazon.awssdk.core.exception.SdkException`. EMRFS is the
   supported S3 client on this image.

If a future EMR base image moves these jar paths, adjust the `SPARK_DIST_CLASSPATH` line. Verify
in-pod with `kubectl -n spark exec deploy/spark-history-server -- ls /usr/share/aws/emr/emrfs/lib`.

> This classpath/SDK friction is the cost of running the EMR Spark image as the daemon. A
> dedicated `apache/spark` + `hadoop-aws` + AWS SDK v2 image reading `s3a://` would avoid it, at
> the cost of building and tracking another image (deferred for now).

## Accessing the UI

The SHS Service is `ClusterIP` (the History Server has **no authentication**, so it is not
exposed publicly):

```bash
kubectl -n spark port-forward svc/spark-history-server 18080:18080
# open http://localhost:18080  → lists finished/failed apps; for a job still running,
# use its driver port-4040 UI instead (see "Viewing a running job" above)
```

To expose it for real, put it behind an authenticated Ingress / ALB — do not switch the Service
to a public LoadBalancer as-is.

## One-time bootstrap: create the event-log prefix

Spark's event-log writer requires the base log directory to exist or the job aborts with
`Log directory s3://... does not exist`. Create the prefix once:

```bash
aws s3api put-object --bucket lakehouse-at-scale-spark-logs --key spark-events/
```

## IAM (shared de-team role)

The SHS reuses the **de-team IRSA role** (no new role):

- `dagster-irsa/de-team-role` trust policy lists `spark:spark-history-server` so that SA can
  assume the role.
- `dagster-irsa/de-team-policy` `LogsBucketReadWrite` grants read **and** write on the spark-logs
  bucket — writers (driver + executors in `eks_client` mode run as this role) need `PutObject`/
  `DeleteObject` for rolling logs; the SHS only reads.
- The `emr_containers` writer path uses the **EMR execution role**, which already has write to the
  spark-logs bucket via `emr-virtual-cluster` `s3_bucket_arns` — no change required.

## Deploy order

`terragrunt apply` the two IRSA units (de-team-policy, de-team-role). Rebuild the de-team **Base
Image** (it bakes the DataFlint JAR) and then the Code Image so jobs pick up the new event-log +
DataFlint Spark properties. Let ArgoCD sync the `spark-history-server` app (sync-wave 4).

The SHS pins the **base image** (`de-team-base:latest`), so it is decoupled from code deploys —
the CI `update-argocd` job bumps only `argocd/dagster/values.yaml`, never
`argocd/spark-history-server/values.yaml`. The only value to keep current here is
`serviceAccount.roleArn` (the de-team role ARN). Rebuilding the de-team Code Image does not change
the SHS image; `latest` only moves when you rebuild the base image.
