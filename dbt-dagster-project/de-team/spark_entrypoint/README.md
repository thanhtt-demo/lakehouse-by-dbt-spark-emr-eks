# Spark Entrypoint (de-team)

> **Scope:** This entrypoint is used **only by the `emr_containers` submit mode**. The default
> mode is now `eks_client`, where dbt runs in-process inside the Dagster step pod (the Spark
> driver in client mode) and `entrypoint.py` is **not** invoked. See
> [`../dagster_project/spark_backends/README.md`](../dagster_project/spark_backends/README.md)
> for the dual-mode architecture and how to toggle between them.

`entrypoint.py` is the script that Spark executes on the EMR on EKS driver pod. It bridges Dagster and dbt-spark: Dagster submits a Spark job pointing at this file, the script runs `dbt build --select <model>` in-process against the active SparkSession, and reports results back to Dagster via [Dagster Pipes](https://docs.dagster.io/concepts/dagster-pipes/).

## Role in the stack

```
Dagster @dbt_assets (user-deployment pod)
    └─► PipesEMRContainersClient.run(start_job_run_params)
            └─► EMR on EKS StartJobRun
                    └─► Spark driver pod boots
                            ├─► spark-submit local:///app/entrypoint.py
                            │       └─► SparkSession.builder.getOrCreate()
                            │       └─► dbtRunner().invoke(["build", ...])
                            │               └─► dbt-spark (method: session)
                            │                       └─► Active SparkSession
                            └─► PipesS3MessageWriter uploads events to S3
                                    └─► Dagster reads asset events + logs
```

The entrypoint is baked into the Code Image at `/app/entrypoint.py` by `Dockerfile.code`. EMR invokes it with `--entry-point local:///app/entrypoint.py` so Spark loads it through the driver's PySpark bootstrap — that's why the script can call `SparkSession.builder.getOrCreate()` and dbt-spark's session method picks up the existing context.

## What the script does

1. Open a `PipesS3MessageWriter` so Dagster can consume log + materialization events.
2. Create the SparkSession (with `enableHiveSupport()` so Glue catalog integration works).
3. Redirect dbt's `target/` and `logs/` to `/tmp` (EMR driver pods have a read-only root filesystem — writing next to `/app/dbt_project` fails with `OSError: Read-only file system`).
4. Invoke `dbtRunner` with `build --select <model>` and the redirected paths.
5. Parse `target/run_results.json`, then:
   - Report each dbt test as an `AssetCheckResult` with the executed SQL attached.
   - Report the model as an `AssetMaterialization` with compiled SQL, executed SQL, test counts, and an EMR console deep link.
6. Flush stdio and sleep briefly to let the stdio forwarder thread upload its last chunk before Pipes closes.
7. Stop Spark in a `finally` block (after Pipes closes, avoiding "message after closed" warnings).

## Environment variables read at runtime

| Variable | Purpose | Default |
|---|---|---|
| `AWS_REGION` | Used to build the EMR console URL in asset metadata | `ap-southeast-1` |
| `EMR_VIRTUAL_CLUSTER_ID` | Passed through to the console link | — |
| `DBT_DEBUG` | When `1`/`true`/`yes`, adds `--debug` to the dbt invocation (verbose SQL + event logs) | off |
| `PIPES_STDIO_FLUSH_SECS` | Grace window (seconds) for the stdio forwarder to flush before Pipes closes | `3` |
| `DBT_TARGET_PATH` / `DBT_LOG_PATH` | Set by the script itself to the `/tmp/dbt-*/` sandbox — not user-configurable | — |

These come from the Dagster user-deployment env block in `argocd/dagster/values.yaml`.

## Extras passed from Dagster

Dagster invokes the EMR job with `extras={...}` on `PipesEMRContainersClient.run`. The entrypoint reads them through `pipes.get_extra(...)`:

| Extra | Set by | Used for |
|---|---|---|
| `model_name` | `dbt_assets.py` per selected asset key | dbt `--select` and asset check routing |
| `dbt_command` | `dbt_assets.py` (`"build"`) | dbt subcommand |

## Capturing SQL for the Dagster UI

`entrypoint.py` does **not** parse dbt stdout or hook into the dbt event bus. It reads the artifact files that dbt writes into `target/` after each invocation:

| Artifact | What it is |
|---|---|
| `target/compiled/<pkg>/models/.../<model>.sql` | Pure Jinja-rendered SELECT for a model |
| `target/run/<pkg>/models/.../<model>.sql` | Full DDL/DML Spark executes (`CREATE OR REPLACE TABLE … USING iceberg AS SELECT …`) |
| `target/compiled/<pkg>/models/.../schema.yml/<test>.sql` | Compiled SQL for each generic test |
| `target/run_results.json` → `results[*].compiled_code` | Same SQL, emitted inline by dbt 1.6+ (preferred, file is fallback) |

The `_read_model_sql` helper `os.walk`s the appropriate subtree and matches by filename — the subfolder depth depends on the dbt package + model path, so we don't hard-code it. The files are always present by the time `dbtRunner.invoke()` returns, so there's no race.

Metadata surfaces in the UI:

- `compiled_sql` and `executed_sql` on the `AssetMaterialization` (asset detail → Materializations tab).
- `test_sql` on each `AssetCheckResult` (asset detail → Checks tab).
- Plus `pipes.log.info(...)` lines so the SQL is searchable in the Run log stream too.

## Test name handling

dbt test `unique_id` has the shape `test.<package>.<test_name>.<hash>` for generic tests and `test.<package>.<test_name>` for singular tests. Dagster registers the asset check under the bare `<test_name>` (from `@dbt_assets`), so the entrypoint extracts `parts[2]` from the unique_id — using the trailing hash would produce `DagsterInvariantViolationError: Received unexpected AssetCheckResult`.

## Read-only filesystem workaround

EMR on EKS 7.x driver pods run with `readOnlyRootFilesystem: true`. Anything dbt tries to write inside the image (`/app/dbt_project/target`, `/app/dbt_project/logs`) fails with `OSError(30, 'Read-only file system')`. The entrypoint creates `/tmp/dbt-<random>/{target,logs}` with `tempfile.mkdtemp(dir="/tmp")`, sets `DBT_TARGET_PATH` + `DBT_LOG_PATH`, and passes `--target-path` / `--log-path` explicitly to `dbtRunner`. `/tmp` is a writable tmpfs on every Kubernetes pod.

## Python version

EMR on EKS 7.13's base image ships Python 3.11 as `PYSPARK_PYTHON` (see `/etc/spark/conf/spark-env.sh`). The `Dockerfile.base` installs dagster-pipes, dbt-core, dbt-spark, boto3 into the **3.11** site-packages via `python3.11 -m pip install ...`. Using the default `pip3` (Python 3.9) would install packages in an interpreter Spark never executes, leading to `ModuleNotFoundError: dagster_pipes` at runtime.

## Why dbt-spark `method: session`

With `method: session`, dbt-spark discovers the active `SparkSession` in the same Python process instead of opening a Thrift / HTTP connection. This makes the Spark driver pod a "dbt + Spark all-in-one" — no extra gateway to manage, dbt can call Spark catalogs and extensions (like Iceberg) directly, and test SQL runs on the same executors as the model build.

The tradeoff: `dbt-spark` validator still requires a `host` field in `profiles.yml` even when `method: session` ignores it. The profile uses `host: localhost` as a placeholder to satisfy validation.

## Failure signalling

If `dbtRunner.invoke` returns `success=False`:

1. The entrypoint scrapes `result.exception` (dbt CLI errors like parse failures) and every non-pass row in `run_results` for the error message.
2. Builds a semicolon-joined detail string and logs it via `pipes.log.error`.
3. Raises `RuntimeError(...)` so the Spark job exits non-zero.
4. EMR marks the job `FAILED` with reason `USER_ERROR`, and Dagster surfaces the full message in the Dagster run log (no need to SSH into the pod or dig through CloudWatch).

## When to rebuild this image

Any change to `entrypoint.py`, `Dockerfile.code`, or the dbt project files triggers a Code Image rebuild through GitHub Actions (`.github/workflows/ci-cd.yml`). Changes to Python dependencies require rebuilding `Dockerfile.base` manually (`docker build -f Dockerfile.base -t … && docker push`) — the CI only rebuilds the Code layer.

Use `scripts/smoke-test-dbt-model.ps1` for fast iteration when debugging entrypoint changes: it uploads a standalone debug runner to S3 and submits an EMR job against an existing Code Image tag, skipping the user-deployment pod entirely.

## Log streaming vs archiving

The Spark driver writes to four log sinks simultaneously — each with a different delay profile. Use the right one for the task:

| Sink | Where | Delay | Use for |
|---|---|---|---|
| `s3://lakehouse-at-scale-spark-logs/emr-on-eks/<vc>/jobs/<job-id>/...` | S3 | Only after job ends | Post-mortem, archive |
| `/emr-on-eks/lakehouse-at-scale` | CloudWatch Logs | ~30–60 s | Near-real-time tail + search |
| Pipes messages → Dagster **Events** tab | Dagster UI | ~10–20 s (poll interval) | Streaming Spark driver stdio inside Dagster |
| `kubectl logs -f spark-<job-id>-driver` | Terminal | Real-time (<1 s) | Live debugging |

**EMR on EKS uploads the S3 archive only once, at job shutdown** — the fluentd sidecar on the job pod buffers the log files locally and ships them when the container exits. Don't rely on S3 for streaming; reach for CloudWatch or `kubectl logs -f` instead.

### Dagster stdout/stderr tabs vs Events tab

Two different channels feed the Dagster UI. They look similar but capture different processes:

| Dagster UI location | What it captures | Storage | Latency |
|---|---|---|---|
| **stdout** / **stderr** tabs | The Dagster **run pod** Python process (orchestration side: submit params, poll EMR) | `S3ComputeLogManager` → `s3://lakehouse-at-scale-data-lake/dagster-compute-logs/...` | `upload_interval: 30 s` while the step is running; final flush at step end |
| **Events** tab | The **Spark driver pod** stdio (dbt logs, compiled SQL, test output) | `PipesS3MessageWriter` → `s3://lakehouse-at-scale-pipes/...` | ~10–20 s (Pipes poll interval) |

For Pipes-based assets (`de_team_dbt_assets`) the run pod does very little work itself — it mostly submits and waits. Almost everything interesting (dbt compile, Spark execute) happens on the Spark driver and arrives through the Events tab, not the stdout tab. If the stdout tab looks empty or trails until job end, that's expected — it's the run pod channel, and the run pod has little to print.

```powershell
# Tail CloudWatch for a specific run
aws logs tail /emr-on-eks/lakehouse-at-scale `
    --follow `
    --log-stream-name-prefix <run_id-or-prefix> `
    --region ap-southeast-1

# Live driver log (needs the driver pod to still be alive)
kubectl logs -f -n spark <spark-job-id>-driver
```

If Pipes events in Dagster arrive in big chunks near the end of the run instead of streaming as dbt prints, dbt or Python may be buffering stdout. Two mitigations are already in place:

- `spark.kubernetes.driverEnv.PYTHONUNBUFFERED=1` is set in `SparkConfigManager.build_start_job_run_params`, so the driver's Python stdio flushes line-by-line.
- `DBT_DEBUG=1` in `values.yaml` makes dbt emit verbose events, keeping log pressure steady rather than bursty.
