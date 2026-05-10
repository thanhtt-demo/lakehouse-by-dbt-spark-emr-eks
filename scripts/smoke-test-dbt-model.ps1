# ---------------------------------------------------------------------------------------------------------------------
# SMOKE TEST DBT MODEL ON EMR ON EKS — de-team
#
# Submits an EMR on EKS Spark job that runs `dbt build --select <MODEL>` using
# the existing de-team Code Image in ECR. The driver executes a thin Python
# runner uploaded to S3 (not the baked-in /app/entrypoint.py), so we bypass
# Dagster Pipes entirely and get dbt stdout/stderr directly in CloudWatch.
#
# Why this is useful:
#   - Verify dbt project changes (profiles, macros, models) before triggering
#     Dagster to rebuild the user-deployment pod.
#   - Reproduce driver-pod errors (read-only FS, missing configs, IAM) against
#     a specific image tag already in ECR.
#   - Fast feedback loop for entrypoint.py / Dockerfile changes — after
#     rebuilding + pushing the Code Image manually or via CI.
#
# Usage:
#   # Default: model stg_raw_orders, latest tag in ECR
#   .\scripts\smoke-test-dbt-model.ps1
#
#   # Different model + pin image tag
#   .\scripts\smoke-test-dbt-model.ps1 -Model orders -Tag abc12345
#
#   # Override target (e.g. after VC/role recreation)
#   .\scripts\smoke-test-dbt-model.ps1 -VirtualClusterId <vc> -ExecutionRoleArn <role>
#
# Prerequisites:
#   - AWS CLI authenticated, region ap-southeast-1
#   - EMR Virtual Cluster RUNNING
#   - dbt project already packaged into the de-team-code image under /app/dbt_project
# ---------------------------------------------------------------------------------------------------------------------

[CmdletBinding()]
param(
    [string]$Model = "stg_raw_orders",
    [string]$Tag,
    [string]$Target = "prod",
    [string]$VirtualClusterId = "7rkf3p438vux13mtz4ul921a4",
    [string]$ExecutionRoleArn = "arn:aws:iam::560503716668:role/lakehouse-at-scale-emr-execution-20260510061545521500000001",
    [string]$Region = "ap-southeast-1",
    [string]$Bucket = "lakehouse-at-scale-pipes"
)

$ErrorActionPreference = "Stop"

$AWS_ACCOUNT_ID = "560503716668"
$ECR_REGISTRY   = "$AWS_ACCOUNT_ID.dkr.ecr.$Region.amazonaws.com"
$CODE_REPO      = "$ECR_REGISTRY/lakehouse-at-scale/de-team-code"

# -------------------------------------------------------------------------
# Resolve image tag
# -------------------------------------------------------------------------
if (-not $Tag) {
    Write-Host "==> Resolving latest tag from ECR repo lakehouse-at-scale/de-team-code..."
    $Tag = aws ecr describe-images `
        --repository-name lakehouse-at-scale/de-team-code `
        --region $Region `
        --query "sort_by(imageDetails,&imagePushedAt)[-1].imageTags[0]" `
        --output text
    if (-not $Tag -or $Tag -eq "None") {
        throw "Could not resolve latest tag from ECR. Pass -Tag explicitly."
    }
}
Write-Host "    image: ${CODE_REPO}:${Tag}"
Write-Host "    model: $Model  (target: $Target)"

# -------------------------------------------------------------------------
# Build the debug runner. Key behaviours:
#   - Redirect target/logs to /tmp (EMR driver pods have readOnlyRootFilesystem).
#   - Invoke dbt via Python API so we can print result.exception + per-node
#     messages directly to stdout (visible in CloudWatch stream under
#     /emr-on-eks/lakehouse-at-scale).
#   - Exit non-zero when dbt fails so EMR reports USER_ERROR, not SUCCESS.
# -------------------------------------------------------------------------
$runnerPy = @'
import json
import os
import sys
import tempfile
import textwrap
import traceback

from pyspark.sql import SparkSession


PROFILES_YAML = textwrap.dedent("""
de_team_lakehouse:
  target: prod
  outputs:
    prod:
      type: spark
      method: session
      host: localhost
      schema: staging
""").lstrip()


def main() -> int:
    model = sys.argv[1] if len(sys.argv) > 1 else "stg_raw_orders"
    target = sys.argv[2] if len(sys.argv) > 2 else "prod"
    print(f"[smoke] model={model} target={target}")
    print(f"[smoke] python={sys.executable}")

    spark = (
        SparkSession.builder
        .appName(f"dbt-smoke-{model}")
        .enableHiveSupport()
        .getOrCreate()
    )
    print(f"[smoke] spark={spark.version}")

    tmp = tempfile.mkdtemp(prefix="dbt-", dir="/tmp")
    target_path = os.path.join(tmp, "target")
    log_path = os.path.join(tmp, "logs")
    profiles_dir = os.path.join(tmp, "profiles")
    os.makedirs(profiles_dir, exist_ok=True)
    with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
        f.write(PROFILES_YAML)
    os.environ["DBT_TARGET_PATH"] = target_path
    os.environ["DBT_LOG_PATH"] = log_path
    print(f"[smoke] target_path={target_path}")
    print(f"[smoke] log_path={log_path}")
    print(f"[smoke] profiles_dir={profiles_dir} (written fresh — bypasses /app)")

    from dbt.cli.main import dbtRunner

    dbt_args = [
        "build",
        "--select", model,
        "--project-dir", "/app/dbt_project",
        "--profiles-dir", profiles_dir,
        "--target", target,
        "--target-path", target_path,
        "--log-path", log_path,
    ]
    print(f"[smoke] dbt args: {' '.join(dbt_args)}")

    try:
        result = dbtRunner().invoke(dbt_args)
    except Exception:
        print("[smoke] dbtRunner raised:")
        traceback.print_exc()
        spark.stop()
        return 2

    print(f"[smoke] success={result.success}")
    if getattr(result, "exception", None):
        print(f"[smoke] exception={result.exception!r}")

    run_results_path = os.path.join(target_path, "run_results.json")
    if os.path.exists(run_results_path):
        with open(run_results_path) as f:
            run_results = json.load(f)
        for r in run_results.get("results", []):
            status = r.get("status", "")
            uid = r.get("unique_id", "")
            msg = r.get("message", "") or ""
            print(f"[smoke] result [{status}] {uid}: {msg}")

    log_file = os.path.join(log_path, "dbt.log")
    if os.path.exists(log_file):
        print("[smoke] --- dbt.log (tail) ---")
        with open(log_file) as f:
            lines = f.readlines()
        for line in lines[-80:]:
            print(line.rstrip())

    spark.stop()
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
'@

# -------------------------------------------------------------------------
# Upload runner to S3 — UTF-8 WITHOUT BOM (AWS CLI refuses BOM in --job-driver)
# -------------------------------------------------------------------------
$utf8NoBom = New-Object System.Text.UTF8Encoding $false

$ts    = Get-Date -Format 'yyyyMMdd-HHmmss'
$s3Key = "smoke-tests/dbt-smoke-$Model-$ts.py"
$entry = "s3://$Bucket/$s3Key"

$tmpPy = New-TemporaryFile
[System.IO.File]::WriteAllText($tmpPy.FullName, $runnerPy, $utf8NoBom)
Write-Host "==> Uploading debug runner to $entry"
aws s3 cp $tmpPy.FullName $entry --region $Region | Out-Null
Remove-Item $tmpPy.FullName -Force

# -------------------------------------------------------------------------
# Build spark-submit parameters. Iceberg runtime jar path comes from EMR
# base image (bundled), extensions must be set for dbt-spark + Iceberg writes.
# -------------------------------------------------------------------------
$sparkParams = @(
    "--jars local:///usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
    "--conf spark.kubernetes.container.image=${CODE_REPO}:${Tag}",
    "--conf spark.driver.cores=1",
    "--conf spark.driver.memory=2g",
    "--conf spark.executor.cores=1",
    "--conf spark.executor.memory=4g",
    "--conf spark.executor.instances=1",
    # Unbuffered Python stdio so dbt log lines stream in near real-time
    # via the Pipes stdio forwarder instead of flushing only at shutdown.
    "--conf spark.kubernetes.driverEnv.PYTHONUNBUFFERED=1",
    # Wrap Spark's session catalog with Iceberg so unqualified `schema.table`
    # refs in dbt resolve through Glue (matches Athena-registered tables).
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog",
    "--conf spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--conf spark.sql.catalog.spark_catalog.warehouse=s3://lakehouse-at-scale-data-lake/warehouse/",
    "--conf spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
) -join " "

$jobDriver = @{
    sparkSubmitJobDriver = @{
        entryPoint            = $entry
        entryPointArguments   = @($Model, $Target)
        sparkSubmitParameters = $sparkParams
    }
} | ConvertTo-Json -Depth 10 -Compress

$configOverrides = @{
    monitoringConfiguration = @{
        cloudWatchMonitoringConfiguration = @{
            logGroupName        = "/emr-on-eks/lakehouse-at-scale"
            logStreamNamePrefix = "smoke-$Model-$ts"
        }
        s3MonitoringConfiguration = @{
            logUri = "s3://lakehouse-at-scale-spark-logs/emr-on-eks/"
        }
    }
} | ConvertTo-Json -Depth 10 -Compress

$tmpDriver = New-TemporaryFile
[System.IO.File]::WriteAllText($tmpDriver.FullName, $jobDriver, $utf8NoBom)
$tmpConfig = New-TemporaryFile
[System.IO.File]::WriteAllText($tmpConfig.FullName, $configOverrides, $utf8NoBom)

$jobName = "smoke-dbt-$Model-$ts"
Write-Host "==> Submitting EMR on EKS job (vc=$VirtualClusterId)..."
$submitOut = aws emr-containers start-job-run `
    --virtual-cluster-id $VirtualClusterId `
    --name $jobName `
    --execution-role-arn $ExecutionRoleArn `
    --release-label "emr-7.13.0-latest" `
    --job-driver "file://$($tmpDriver.FullName)" `
    --configuration-overrides "file://$($tmpConfig.FullName)" `
    --region $Region
Remove-Item $tmpDriver.FullName -Force
Remove-Item $tmpConfig.FullName -Force

$jobId = ($submitOut | ConvertFrom-Json).id
Write-Host "    jobRunId: $jobId"
Write-Host "    cloudwatch logs: /emr-on-eks/lakehouse-at-scale (prefix: smoke-$Model-$ts)"

# -------------------------------------------------------------------------
# Poll until terminal state
# -------------------------------------------------------------------------
Write-Host ""
Write-Host "==> Polling until terminal state..."
$terminal = @("COMPLETED", "FAILED", "CANCELLED", "CANCEL_PENDING")
while ($true) {
    $state = aws emr-containers describe-job-run `
        --virtual-cluster-id $VirtualClusterId `
        --id $jobId `
        --region $Region `
        --query "jobRun.state" `
        --output text
    Write-Host "    state=$state"
    if ($terminal -contains $state) { break }
    Start-Sleep -Seconds 15
}

Write-Host ""
if ($state -eq "COMPLETED") {
    Write-Host "==> SMOKE TEST PASSED. jobId=$jobId"
    Write-Host "    driver stdout: aws s3 cp s3://lakehouse-at-scale-spark-logs/emr-on-eks/$VirtualClusterId/jobs/$jobId/containers/spark-$jobId/spark-$jobId-driver/stdout.gz -"
    exit 0
} else {
    $details = aws emr-containers describe-job-run `
        --virtual-cluster-id $VirtualClusterId `
        --id $jobId `
        --region $Region `
        --query "jobRun.{Reason:failureReason,Details:stateDetails}" `
        --output json
    Write-Host "==> SMOKE TEST FAILED. jobId=$jobId"
    Write-Host $details
    Write-Host ""
    Write-Host "To fetch driver stdout (where [smoke] lines print) once logs sync:"
    Write-Host "  aws s3 cp s3://lakehouse-at-scale-spark-logs/emr-on-eks/$VirtualClusterId/jobs/$jobId/containers/spark-$jobId/spark-$jobId-driver/stdout.gz - --region $Region | gzip -d"
    exit 1
}
