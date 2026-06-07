# Spark History Server (Spark UI)

A single standalone Spark History Server (SHS) serves the Spark UI for **both** finished/failed
jobs and currently-running jobs, by replaying Spark **event logs** from S3. It works the same for
both execution backends (`eks_client` and `emr_containers`).

## How it works

```
 Spark driver (step pod / EMR pod)                Spark History Server (spark ns)
 spark.eventLog.dir = s3://.../spark-events/  ──▶  s3://.../spark-events/  ──▶  Spark UI :18080
 rolling event logs, flushed periodically         polls every 15s (update.interval)
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

A running job flushes rolling event-log chunks while it executes; the SHS rescans S3 every
`spark.history.fs.update.interval` (15s). The job appears under **"Show incomplete applications"**
with ~15–30s latency — within the accepted window. No per-driver port-4040 ingress needed.

## Accessing the UI

The SHS Service is `ClusterIP` (the History Server has **no authentication**, so it is not
exposed publicly):

```bash
kubectl -n spark port-forward svc/spark-history-server 18080:18080
# open http://localhost:18080  → tick "Show incomplete applications" to see running jobs
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

`terragrunt apply` the two IRSA units (de-team-policy, de-team-role), rebuild/redeploy the Code
Image so jobs pick up the new event-log Spark properties, then let ArgoCD sync the
`spark-history-server` app (sync-wave 4). Keep its `image.tag` and `serviceAccount.roleArn` in
`argocd/spark-history-server/values.yaml` in lock-step with the Dagster user-deployment image and
the de-team role ARN.
