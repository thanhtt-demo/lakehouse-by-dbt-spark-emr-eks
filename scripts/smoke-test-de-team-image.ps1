# ---------------------------------------------------------------------------------------------------------------------
# SMOKE TEST EMR SPARK JOB — de-team code image
#
# Submits a minimal Spark job to EMR on EKS using an existing de-team Code Image
# in ECR. Verifies the Spark driver can:
#   - Start with Iceberg extensions loaded from --jars
#   - Import dagster_pipes, dbt, boto3 in the Spark driver Python (python3.11)
#   - Create and stop a SparkSession
#
# Does NOT build or push images. Assumes the image tag already exists in ECR.
# Use smoke-test-de-team-image.local.sh (or docker build manually) first if you
# need to rebuild.
#
# Usage:
#   # Use a specific Code Image tag already in ECR
#   .\scripts\smoke-test-de-team-image.ps1 -Tag 81421f60
#
#   # Default: resolves to the latest-pushed tag in the de-team-code repo
#   .\scripts\smoke-test-de-team-image.ps1
#
# Prerequisites:
#   - AWS CLI authenticated (default profile), region ap-southeast-1
#   - EMR Virtual Cluster RUNNING
#   - S3 bucket used below (for the inline entry-point script) writable by caller
# ---------------------------------------------------------------------------------------------------------------------

[CmdletBinding()]
param(
    [string]$Tag,
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
Write-Host "    using image: ${CODE_REPO}:${Tag}"

# -------------------------------------------------------------------------
# Upload minimal entry-point to S3. entryPoint paths must be a runnable
# object — we override the baked-in /app/entrypoint.py (which expects Pipes
# env vars) with an inline smoke test.
# -------------------------------------------------------------------------
$ts    = Get-Date -Format 'yyyyMMdd-HHmmss'
$s3Key = "smoke-tests/smoke-$ts.py"
$entry = "s3://$Bucket/$s3Key"

$inlinePy = @'
import sys
import dagster_pipes
import boto3
from dbt.version import __version__ as dbt_version
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("smoke")
    .enableHiveSupport()
    .getOrCreate()
)

print("python:", sys.executable)
print("dagster_pipes:", dagster_pipes.__file__)
print("dbt:", dbt_version)
print("boto3:", boto3.__version__)
print("spark:", spark.version)

spark.stop()
'@

$tmpPy = New-TemporaryFile
# PowerShell's Set-Content -Encoding utf8 prepends a BOM which the AWS CLI refuses
# to parse. Use .NET's UTF8Encoding(false) to write UTF-8 without BOM for both
# the Python entry-point and the start-job-run JSON payload.
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($tmpPy.FullName, $inlinePy, $utf8NoBom)
Write-Host "==> Uploading smoke script to $entry"
aws s3 cp $tmpPy.FullName $entry --region $Region | Out-Null
Remove-Item $tmpPy.FullName -Force

# -------------------------------------------------------------------------
# Build StartJobRun payload
# -------------------------------------------------------------------------
$sparkParams = @(
    "--jars local:///usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
    "--conf spark.kubernetes.container.image=${CODE_REPO}:${Tag}",
    "--conf spark.driver.cores=1",
    "--conf spark.driver.memory=2g",
    "--conf spark.executor.cores=1",
    "--conf spark.executor.memory=2g",
    "--conf spark.executor.instances=1",
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
) -join " "

$jobDriverJson = @{
    sparkSubmitJobDriver = @{
        entryPoint            = $entry
        sparkSubmitParameters = $sparkParams
    }
} | ConvertTo-Json -Depth 10 -Compress

$tmpDriver = New-TemporaryFile
[System.IO.File]::WriteAllText($tmpDriver.FullName, $jobDriverJson, $utf8NoBom)

$jobName = "smoke-$ts"
Write-Host "==> Submitting EMR on EKS job (vc=$VirtualClusterId)..."
$submitOut = aws emr-containers start-job-run `
    --virtual-cluster-id $VirtualClusterId `
    --name $jobName `
    --execution-role-arn $ExecutionRoleArn `
    --release-label "emr-7.13.0-latest" `
    --job-driver "file://$($tmpDriver.FullName)" `
    --region $Region
Remove-Item $tmpDriver.FullName -Force

$jobId = ($submitOut | ConvertFrom-Json).id
Write-Host "    jobRunId: $jobId"

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
    exit 1
}
