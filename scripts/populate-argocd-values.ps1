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
    $tmpOut = [System.IO.Path]::GetTempFileName()
    $tmpErr = [System.IO.Path]::GetTempFileName()
    try {
        $proc = Start-Process -FilePath "terragrunt" `
            -ArgumentList "output","-raw","--terragrunt-no-color",$OutputName `
            -WorkingDirectory $fullPath `
            -RedirectStandardOutput $tmpOut `
            -RedirectStandardError $tmpErr `
            -NoNewWindow -Wait -PassThru
        $stdout = if (Test-Path $tmpOut) { Get-Content $tmpOut -Raw } else { "" }
        $stderr = if (Test-Path $tmpErr) { Get-Content $tmpErr -Raw } else { "" }
        if ($stdout) { $stdout = $stdout.Trim() }
        if ($stderr) { $stderr = $stderr.Trim() }
        # terragrunt output -raw writes value to stdout
        # but some versions write it to stderr along with logs
        if (-not [string]::IsNullOrWhiteSpace($stdout)) {
            return $stdout
        }
        # Fallback: parse stderr for the actual value (last non-empty line without timestamp)
        if ($stderr) {
            $lines = $stderr -split "`n" | ForEach-Object { $_.Trim() } | Where-Object {
                $_ -and $_ -notmatch "^\d{2}:\d{2}:\d{2}" -and $_ -notmatch "^time=" -and $_ -notmatch "^WARN" -and $_ -notmatch "^INFO" -and $_ -notmatch "^ERROR"
            }
            if ($lines) {
                $val = ($lines | Select-Object -Last 1).Trim()
                if (-not [string]::IsNullOrWhiteSpace($val)) { return $val }
            }
        }
        throw "terragrunt output empty for $OutputName in $ModulePath`nstdout: $stdout`nstderr: $stderr"
    }
    finally {
        Remove-Item $tmpOut, $tmpErr -Force -ErrorAction SilentlyContinue
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
$DE_TEAM_IRSA_ROLE_ARN = Get-TerragruntOutput "$INFRA_DIR\dagster-irsa\de-team-role" "iam_role_arn"
$SALES_TEAM_IRSA_ROLE_ARN = Get-TerragruntOutput "$INFRA_DIR\dagster-irsa\sales-team-role" "iam_role_arn"
Write-Host "    de_team_irsa_role_arn=$DE_TEAM_IRSA_ROLE_ARN"
Write-Host "    sales_team_irsa_role_arn=$SALES_TEAM_IRSA_ROLE_ARN"

Write-Host ""
Write-Host "==> Replacing placeholders in ArgoCD files..."

$FILES = @(
    "$REPO_ROOT\argocd\karpenter\values.yaml",
    "$REPO_ROOT\argocd\karpenter\templates\nodepool-spark-executors.yaml",
    "$REPO_ROOT\argocd\karpenter\templates\nodepool-spark-drivers.yaml",
    "$REPO_ROOT\argocd\dagster\values.yaml"
)

foreach ($file in $FILES) {
    if (-not (Test-Path $file)) {
        Write-Host "    WARN: $file not found, skipping"
        continue
    }

    $content = Get-Content $file -Raw
    $content = $content -replace "PLACEHOLDER_CLUSTER_NAME", $CLUSTER_NAME
    $content = $content -replace "PLACEHOLDER_CLUSTER_ENDPOINT", $CLUSTER_ENDPOINT
    $content = $content -replace "PLACEHOLDER_INTERRUPTION_QUEUE", $INTERRUPTION_QUEUE
    $content = $content -replace "PLACEHOLDER_KARPENTER_CONTROLLER_ROLE_ARN", $KARPENTER_CONTROLLER_ROLE_ARN
    $content = $content -replace "PLACEHOLDER_AWS_ACCOUNT_ID", $AWS_ACCOUNT_ID
    $content = $content -replace "PLACEHOLDER_DE_TEAM_IRSA_ROLE_ARN", $DE_TEAM_IRSA_ROLE_ARN
    $content = $content -replace "PLACEHOLDER_SALES_TEAM_IRSA_ROLE_ARN", $SALES_TEAM_IRSA_ROLE_ARN
    Set-Content $file -Value $content -NoNewline

    Write-Host "    OK $file"
}

Write-Host ""
Write-Host "==> Creating pull request..."

$BRANCH = "chore/populate-argocd-values-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
git checkout -b $BRANCH
git add argocd/
git commit -m "chore: populate ArgoCD values from Terraform outputs

Replaced PLACEHOLDER_* values with actual infrastructure outputs:
- Cluster: $CLUSTER_NAME
- Karpenter IAM role, SQS queue
- Dagster IRSA roles (de-team, sales-team)
- ECR repository URLs (account: $AWS_ACCOUNT_ID)"

git push -u origin $BRANCH

gh pr create `
    --title "chore: populate ArgoCD values from Terraform outputs" `
    --body "## What
Replaced all ``PLACEHOLDER_*`` values in ArgoCD Helm charts with actual Terraform outputs.

## Placeholders replaced
| Placeholder | Value |
|---|---|
| ``PLACEHOLDER_CLUSTER_NAME`` | ``$CLUSTER_NAME`` |
| ``PLACEHOLDER_CLUSTER_ENDPOINT`` | ``$CLUSTER_ENDPOINT`` |
| ``PLACEHOLDER_INTERRUPTION_QUEUE`` | ``$INTERRUPTION_QUEUE`` |
| ``PLACEHOLDER_KARPENTER_CONTROLLER_ROLE_ARN`` | ``$KARPENTER_CONTROLLER_ROLE_ARN`` |
| ``PLACEHOLDER_AWS_ACCOUNT_ID`` | ``$AWS_ACCOUNT_ID`` |
| ``PLACEHOLDER_DE_TEAM_IRSA_ROLE_ARN`` | ``$DE_TEAM_IRSA_ROLE_ARN`` |
| ``PLACEHOLDER_SALES_TEAM_IRSA_ROLE_ARN`` | ``$SALES_TEAM_IRSA_ROLE_ARN`` |

## Files changed
- ``argocd/karpenter/values.yaml``
- ``argocd/karpenter/templates/nodepool-spark-executors.yaml``
- ``argocd/karpenter/templates/nodepool-spark-drivers.yaml``
- ``argocd/dagster/values.yaml``

Generated by ``scripts/populate-argocd-values.ps1``" `
    --base main

Write-Host ""
Write-Host "==> Done! Pull request created on branch: $BRANCH"
