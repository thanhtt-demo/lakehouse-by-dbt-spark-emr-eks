# Tech Stack

## Infrastructure as Code

- **Terraform** — AWS resource modules (VPC, S3, IAM, Glue, ECR)
- **Terraform Community Modules** — Prefer `terraform-aws-modules/*` at pinned versions over custom implementations. Only write custom modules when community modules don't cover the use case. Always pin exact versions.
  - `terraform-aws-modules/eks/aws` v21.19.0 — EKS cluster
  - `terraform-aws-modules/eks/aws//modules/karpenter` v21.19.0 — Karpenter IAM/SQS
  - `terraform-aws-modules/emr/aws//modules/virtual-cluster` v3.3.0 — EMR Virtual Cluster on EKS (includes K8s RBAC, IAM execution role, CloudWatch log group)
  - `terraform-aws-modules/ecr/aws` v3.2.0 — ECR repositories
  - `terraform-aws-modules/s3-bucket/aws` v5.12.0 — S3 buckets (one Terragrunt unit per bucket, no local wrapper)
  - `terraform-aws-modules/iam/aws//modules/iam-policy` v6.6.0 — IAM custom policies
  - `terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts` v6.6.0 — IRSA roles for Dagster service accounts
- **Terragrunt** — Orchestrates Terraform modules, manages remote state (S3 + DynamoDB), generates provider blocks
  - Root config pattern from [terragrunt-ecs-msk-connect-stask](https://github.com/thanhtt-demo/terragrunt-ecs-msk-connect-stask)
  - Can reference community modules directly via `tfr:///` source syntax

## Application Stack

- **Dagster** — Orchestrator. Assets, resources, schedules, sensors
- **dagster-dbt** — `@dbt_assets` decorator, `DbtProject`, `DbtCliResource` for dbt integration
- **dagster-aws** — `PipesEMRContainersClient`, `PipesS3MessageReader` for EMR on EKS integration
- **dagster-pipes** — `PipesS3MessageWriter` (runs inside Spark job)
- **dbt-core** — Data transformation framework
- **dbt-spark** — Spark adapter (de-team), uses `method: session`
- **dbt-athena** — Athena adapter (sales-team)
- **Apache Spark** — Compute engine, runs on EMR on EKS
- **Apache Iceberg** — Table format (ACID, time travel, schema evolution)
- **Python 3.10** — Runtime

## AWS Services

- EKS (Kubernetes), EMR on EKS (Spark), S3 (data lake + state), Glue Data Catalog (metastore), ECR (container images), IAM, DynamoDB (state locking)

## Deployment & GitOps

- **ArgoCD** — App-of-Apps pattern (Helm-based), sync waves for ordered deployment
  - Pattern from [argocd-spark-operator](https://github.com/thanhtt-demo/argocd-spark-operator)
- **Helm** — Dagster umbrella chart wrapping official Dagster Helm chart
- **GitHub Actions** — CI/CD pipeline (build Code Image, update ArgoCD repo)
- **Karpenter** — EKS node autoscaling (Spot for executors, On-Demand for drivers)

## Testing

- **Hypothesis** — Property-based testing for application logic (SparkConfigManager)
- **pytest** — Unit and integration tests
- `terraform validate` / `terraform plan` — Infrastructure validation
- `helm template` — Kubernetes manifest validation

## Common Commands

```bash
# Terraform / Terragrunt
terragrunt plan                          # Preview changes for a module
terragrunt apply                         # Apply changes for a module
terragrunt run-all plan                  # Plan all modules in a directory
terragrunt graph-dependencies            # Show module dependency graph
terraform validate                       # Validate a Terraform module

# Testing (application code repo)
pytest tests/ -v --hypothesis-seed=0     # Run all tests (unit + property-based)
pytest tests/property/ -v                # Run property-based tests only

# Helm
helm template argocd/dagster/ --debug    # Validate Dagster Helm chart

# Docker
docker build -f Dockerfile.base -t base:test .   # Build base image
docker build -f Dockerfile.code -t code:test .    # Build code image

# dbt
dagster-dbt project prepare-and-package  # Precompile dbt manifest for production
```
