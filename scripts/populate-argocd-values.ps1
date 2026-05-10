# ---------------------------------------------------------------------------------------------------------------------
# POPULATE ARGOCD VALUES (PowerShell)
# Reads Terraform outputs via Terragrunt and replaces PLACEHOLDER_* values in ArgoCD files.
# Creates a Git branch and opens a pull request via GitHub CLI (gh).
#
# Prerequisites:
#   - AWS CLI configured with profile "non-prod"
#   - Terragrunt installed
#   - GitHub CLI (gh) installed and authenticated
#   - All Terraform modules applied (EKS, Karpenter, ECR, IRSA)
#
# Usage:
#   .\scripts\populate-argocd-values.ps1
# ---------------------------------------------------------------------------------------------------------------------

$ErrorActionPreference = "Stop"

$INFRA_DIR = "infra\non-prod\ap-southeast-1"
$REPO_ROOT = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

function Get-TerragruntOutput {
    param([string]$ModulePath, [string]$OutputName)
    $fullPath = Join-Path $REPO_ROOT $ModulePath
    $originalLocation = Get-Location
    $tmpErr = [System.IO.Path]::GetTempFileName()
    try {
        Set-Location $fullPath
        # Use cmd /c to avoid PowerShell treating stderr as terminating error
        $result = cmd /c "terragrunt output -raw $OutputName 2>`"$tmpErr`""
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($result)) {
            $stderrContent = if (Test-Path $tmpErr) { Get-Content $tmpErr -Raw } else { "" }
            throw "terragrunt output failed for $OutputName in $ModulePath (exit: $LASTEXITCODE)`nstderr: $stderrContent"
        }
        return $result.Trim()
    }
    finally {
        Set-Location $originalLocation
        Remove-Item $tmpErr -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "==> Fetching Terraform outputs..."

# EKS outputs
$CLUSTER_NAME = Get-TerragruntOutput "$INFRA_DIR\eks" "cluster_name"
$CLUSTER_ENDPOINT = Get-TerragruntOutput "$INFRA_DIR\eks" "cluster_endpoint"
Write-Host "    cluster_name=$CLUSTER_NAME"
Write-Host "    cluster_endpoint=$CLUSTER_ENDPOINT"

# Karpenter outputs
$KARPENTER_CONTROLLER_ROLE_ARN = Get-TerragruntOutput "$INFRA_DIR\karpenter" "iam_role_arn"
$INTERRUPTION_QUEUE = Get-TerragruntOutput "$INFRA_DIR\karpenter" "queue_name"
Write-Host "    karpenter_controller_role_arn=$KARPENTER_CONTROLLER_ROLE_ARN"
Write-Host "    interruption_queue=$INTERRUPTION_QUEUE"

# AWS Account ID
$AWS_ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text --profile non-prod).Trim()
Write-Host "    aws_account_id=$AWS_ACCOUNT_ID"

# Dagster IRSA role ARNs
$DE_TEAM_IRSA_ROLE_ARN = Get-TerragruntOutput "$INFRA_DIR\dagster-irsa\de-team-role" "arn"
$SALES_TEAM_IRSA_ROLE_ARN = Get-TerragruntOutput "$INFRA_DIR\dagster-irsa\sales-team-role" "arn"
Write-Host "    de_team_irsa_role_arn=$DE_TEAM_IRSA_ROLE_ARN"
Write-Host "    sales_team_irsa_role_arn=$SALES_TEAM_IRSA_ROLE_ARN"

# EMR Virtual Cluster outputs (consumed by de-team Dagster pod to submit Spark jobs)
$EMR_VIRTUAL_CLUSTER_ID = Get-TerragruntOutput "$INFRA_DIR\emr-virtual-cluster" "virtual_cluster_id"
$EMR_EXECUTION_ROLE_ARN = Get-TerragruntOutput "$INFRA_DIR\emr-virtual-cluster" "job_execution_role_arn"
Write-Host "    emr_virtual_cluster_id=$EMR_VIRTUAL_CLUSTER_ID"
Write-Host "    emr_execution_role_arn=$EMR_EXECUTION_ROLE_ARN"

Write-Host ""
Write-Host "==> Updating ArgoCD files with Terraform outputs..."

# --- argocd/karpenter/values.yaml ---
$karpenterValues = "$REPO_ROOT\argocd\karpenter\values.yaml"
if (Test-Path $karpenterValues) {
    $content = Get-Content $karpenterValues -Raw
    $content = $content -replace '(clusterName:\s*)"[^"]*"', "`$1`"$CLUSTER_NAME`""
    $content = $content -replace '(clusterEndpoint:\s*)"[^"]*"', "`$1`"$CLUSTER_ENDPOINT`""
    $content = $content -replace '(interruptionQueue:\s*)"[^"]*"', "`$1`"$INTERRUPTION_QUEUE`""
    $content = $content -replace '(eks\.amazonaws\.com/role-arn:\s*)"[^"]*"', "`$1`"$KARPENTER_CONTROLLER_ROLE_ARN`""
    Set-Content $karpenterValues -Value $content -NoNewline
    Write-Host "    OK $karpenterValues"
}

# --- argocd/karpenter/templates/nodepool-spark-drivers.yaml ---
$driversYaml = "$REPO_ROOT\argocd\karpenter\templates\nodepool-spark-drivers.yaml"
if (Test-Path $driversYaml) {
    $content = Get-Content $driversYaml -Raw
    $content = $content -replace '(karpenter\.sh/discovery:\s*)"[^"]*"', "`$1`"$CLUSTER_NAME`""
    Set-Content $driversYaml -Value $content -NoNewline
    Write-Host "    OK $driversYaml"
}

# --- argocd/karpenter/templates/nodepool-spark-executors.yaml ---
$executorsYaml = "$REPO_ROOT\argocd\karpenter\templates\nodepool-spark-executors.yaml"
if (Test-Path $executorsYaml) {
    $content = Get-Content $executorsYaml -Raw
    $content = $content -replace '(karpenter\.sh/discovery:\s*)"[^"]*"', "`$1`"$CLUSTER_NAME`""
    Set-Content $executorsYaml -Value $content -NoNewline
    Write-Host "    OK $executorsYaml"
}

# --- argocd/dagster/values.yaml ---
$dagsterValues = "$REPO_ROOT\argocd\dagster\values.yaml"
if (Test-Path $dagsterValues) {
    $content = Get-Content $dagsterValues -Raw
    # IRSA role ARN for user-deployments service account
    $content = $content -replace '(eks\.amazonaws\.com/role-arn:\s*)"[^"]*"', "`$1`"$DE_TEAM_IRSA_ROLE_ARN`""
    # ECR image repository URLs (replace account ID in existing ECR URLs)
    $content = $content -replace '(\d{12})(\.dkr\.ecr\.)', "$AWS_ACCOUNT_ID`$2"
    # EMR Virtual Cluster ID + execution role ARN (de-team env vars)
    $content = $content -replace '(?ms)(-\s+name:\s+EMR_VIRTUAL_CLUSTER_ID\s*\r?\n\s+value:\s*)"[^"]*"', "`$1`"$EMR_VIRTUAL_CLUSTER_ID`""
    $content = $content -replace '(?ms)(-\s+name:\s+EMR_EXECUTION_ROLE_ARN\s*\r?\n\s+value:\s*)"[^"]*"', "`$1`"$EMR_EXECUTION_ROLE_ARN`""
    Set-Content $dagsterValues -Value $content -NoNewline
    Write-Host "    OK $dagsterValues"
}

Write-Host ""
Write-Host "==> Creating pull request..."

$BRANCH = "chore/update-argocd-values-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
git checkout -b $BRANCH
git add argocd/
git commit -m "chore: update ArgoCD values from Terraform outputs

Updated infrastructure values from current Terraform state:
- Cluster: $CLUSTER_NAME
- Karpenter IAM role, SQS queue
- Dagster IRSA role (de-team)
- EMR Virtual Cluster: $EMR_VIRTUAL_CLUSTER_ID
- EMR execution role: $EMR_EXECUTION_ROLE_ARN
- ECR account ID: $AWS_ACCOUNT_ID"

git push -u origin $BRANCH

gh pr create `
    --title "chore: update ArgoCD values from Terraform outputs" `
    --body "## What
Updated ArgoCD Helm chart values with current Terraform outputs.

## Values applied
| Key | Value |
|---|---|
| clusterName | ``$CLUSTER_NAME`` |
| clusterEndpoint | ``$CLUSTER_ENDPOINT`` |
| interruptionQueue | ``$INTERRUPTION_QUEUE`` |
| karpenter role-arn | ``$KARPENTER_CONTROLLER_ROLE_ARN`` |
| AWS Account ID | ``$AWS_ACCOUNT_ID`` |
| de-team IRSA role | ``$DE_TEAM_IRSA_ROLE_ARN`` |
| EMR Virtual Cluster ID | ``$EMR_VIRTUAL_CLUSTER_ID`` |
| EMR execution role ARN | ``$EMR_EXECUTION_ROLE_ARN`` |

## Files changed
- ``argocd/karpenter/values.yaml``
- ``argocd/karpenter/templates/nodepool-spark-drivers.yaml``
- ``argocd/karpenter/templates/nodepool-spark-executors.yaml``
- ``argocd/dagster/values.yaml``

Generated by ``scripts/populate-argocd-values.ps1``" `
    --base main

Write-Host ""
Write-Host "==> Done! Pull request created on branch: $BRANCH"
