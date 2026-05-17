# Debugging & Smoke Testing

Scripts to verify de-team Base + Code images before going through the full Dagster -> EMR run path. Use when iterating on Dockerfile.base, Python dependencies, or Spark configuration.

## Local Build + Import Check

**Script:** `scripts/smoke-test-de-team-image.local.sh`

Builds the Base Image and runs a quick import test using `python3.11` (matching EMR on EKS 7.13). No ECR push, no AWS calls.

```bash
# Default tag (de-team-base:local-smoke)
./scripts/smoke-test-de-team-image.local.sh

# Custom tag
./scripts/smoke-test-de-team-image.local.sh my-tag
```

Use as a fast feedback loop when changing Dockerfile.base or pinned package versions. Runs in Git Bash / WSL / Linux.

## Remote EMR on EKS Job Submit

**Script:** `scripts/smoke-test-de-team-image.ps1`

Submits a minimal Spark job to the existing EMR Virtual Cluster using a Code Image tag already in ECR. Verifies end-to-end that the Spark driver can:

- Start with Iceberg extensions from --jars
- Import dagster_pipes, dbt, boto3 in python3.11
- Create and stop a SparkSession

Does **not** build or push images. Does **not** touch the running Dagster deployment.

```powershell
# Default: resolves the latest tag in lakehouse-at-scale/de-team-code
.\scripts\smoke-test-de-team-image.ps1

# Pin a specific tag already in ECR
.\scripts\smoke-test-de-team-image.ps1 -Tag 81421f60

# Override defaults (e.g. after VC or execution role is recreated)
.\scripts\smoke-test-de-team-image.ps1 `
    -Tag 81421f60 `
    -VirtualClusterId <vc-id> `
    -ExecutionRoleArn <role-arn>
```

The script uploads an inline smoke script to S3, submits the EMR job, then polls until terminal state. COMPLETED = pass; on FAILED it prints failureReason + stateDetails.

## Remote dbt Build on EMR on EKS

**Script:** `scripts/smoke-test-dbt-model.ps1`

Submits a Spark job that runs dbt build --select <model> using a Code Image already in ECR. The driver uses a debug runner uploaded to S3 (not the baked-in /app/entrypoint.py) and ships logs to CloudWatch + S3. Dagster Pipes is bypassed entirely so you see full dbt output.

The runner also:
- Writes a fresh profiles.yml to /tmp (no Code Image rebuild needed)
- Redirects dbt target/ and logs/ to /tmp (EMR driver pods have read-only root filesystem)

**When to use:**
- Validating changes to entrypoint.py, profiles.yml, Dockerfile, or dbt models without round-tripping through Dagster
- Reproducing a production dbt error with the exact same image + IAM + Glue catalog

```powershell
# Default: dbt build stg_raw_orders against the latest tag in ECR
.\scripts\smoke-test-dbt-model.ps1

# Different model + pin image tag
.\scripts\smoke-test-dbt-model.ps1 -Model orders -Tag abc12345

# After VC or execution role recreation
.\scripts\smoke-test-dbt-model.ps1 `
    -Model stg_raw_orders `
    -VirtualClusterId <vc> `
    -ExecutionRoleArn <role>
```

When the job fails, the script prints the CloudWatch log group/prefix and an aws s3 cp one-liner to fetch driver stdout once EMR syncs logs.

## Docker Images (Local Build)

Base Images are built and pushed to ECR by the Terraform docker-image module. Code Images are built by the CI/CD pipeline on push to main. Commands below are only for local testing.

```bash
# de-team base image
cd dbt-dagster-project/de-team
docker build -f Dockerfile.base -t de-team-base:latest .

# de-team code image
docker build -f Dockerfile.code --build-arg BASE_IMAGE=de-team-base:latest -t de-team-code:latest .

# sales-team base image
cd dbt-dagster-project/sales-team
docker build -f Dockerfile.base -t sales-team-base:latest .

# sales-team code image
docker build -f Dockerfile.code --build-arg BASE_IMAGE=sales-team-base:latest -t sales-team-code:latest .
```
