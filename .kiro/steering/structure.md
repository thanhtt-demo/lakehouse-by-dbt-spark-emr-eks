# Project Structure

This repository is the **infrastructure repo** (Repo 1 of 3). It contains Terraform modules and Terragrunt configurations for provisioning AWS resources.

Two other repositories exist outside this codebase:
- **ArgoCD App Repo** — Helm charts and Kubernetes manifests for ArgoCD deployments
- **Application Code Repo** — dbt projects and Dagster code for both team code locations

## Directory Layout

```
infra/
├── terragrunt.hcl              # Root config: remote state (S3 + DynamoDB), AWS provider generation, global inputs
├── _envcommon/                  # Shared Terragrunt configs included by live environments
│   └── {service}.hcl           # e.g. vpc.hcl — sets terraform source + common inputs
├── modules/                    # Reusable Terraform modules
│   └── {service}/              # e.g. vpc/ — contains main.tf, variables.tf, outputs.tf
└── non-prod/                   # Live environment configs
    └── {account_id}/           # AWS account ID
        └── {region}/           # AWS region
            ├── env.hcl         # Account/region variables (aws_account_id, aws_profile, aws_region)
            └── {service}/      # e.g. vpc/
                └── terragrunt.hcl  # Includes root + _envcommon, optional per-env overrides
```

## Conventions

### Terragrunt

- **Root `terragrunt.hcl`** configures S3 remote state, DynamoDB locking, and generates the AWS provider block with default tags
- **`_envcommon/{service}.hcl`** files set the `terraform.source` to the corresponding module and define shared input defaults
- **Live configs** (`non-prod/{account}/{region}/{service}/terragrunt.hcl`) include root + envcommon, and optionally override inputs
- **`env.hcl`** at the region level provides `aws_account_id`, `aws_profile`, and `aws_region` — loaded by root config via `read_terragrunt_config(find_in_parent_folders("env.hcl"))`
- State bucket naming: `lakehouse-at-scale-tfstate-{account_id}`
- Lock table: `lakehouse-at-scale-tfstate-lock`
- **`dependency` blocks** must always include `mock_outputs` with `mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]` so that `terragrunt run --all plan` works before upstream modules are applied

### Terraform vs ArgoCD Boundary

- **Terraform/Terragrunt** manages AWS resources only: VPC, EKS cluster, IAM roles, SQS queues, S3 buckets, ECR repos, Glue databases, etc.
- **ArgoCD** manages Kubernetes resources: Helm charts (Karpenter, Dagster), CRDs (NodePool, EC2NodeClass), namespaces, etc.
- Rule of thumb: if it's an AWS API resource → Terraform. If it's a Kubernetes manifest → ArgoCD.

### Terraform Modules

- Prefer community modules from `terraform-aws-modules/*` at the latest version. Only create local wrapper modules when you need to combine multiple resources that the community module doesn't cover (e.g. Karpenter: community submodule for IAM/SQS + Helm release + kubectl CRDs)
- When using community modules directly, reference them from `_envcommon/{service}.hcl` via `tfr:///` source syntax with pinned version
- Local modules live in `modules/{service}/` with `main.tf`, `variables.tf`, `outputs.tf`
- Resources use `var.name` as prefix for naming (e.g. `${var.name}-vpc`)
- Tags are merged via `merge(var.tags, { Name = "..." })` pattern
- All modules accept a `tags` variable (map of strings) for additional tagging
- Default tags (`ManagedBy`, `Project`, `Environment`) are set in the provider block by Terragrunt

### Module Dependency Order

VPC → EKS → Karpenter → S3 → ECR → EMR Virtual Cluster (needs EKS + S3) → IAM (needs EKS + S3) → Glue (needs S3) → Docker Images

### Comment Style

- Use banner-style comment blocks with dashes for section headers in both `.tf` and `.hcl` files
- Example:
  ```hcl
  # ---------------------------------------------------------------------------------------------------------------------
  # SECTION TITLE
  # Description of what this section does.
  # ---------------------------------------------------------------------------------------------------------------------
  ```

### Planned Modules (from design spec)

The following local modules are planned but not yet implemented: `docker-image`.

### Terragrunt Unit Naming

Each AWS resource or logical group gets its own Terragrunt unit (directory with `terragrunt.hcl`). Related resources are grouped under a parent folder:
- S3 buckets: `s3/data-lake/`, `s3/pipes/`, `s3/spark-logs/` — share one envcommon `s3.hcl`
- Dagster IRSA: `dagster-irsa/de-team-policy/`, `dagster-irsa/de-team-role/`, etc. — share `dagster-irsa-policy.hcl` and `dagster-irsa-role.hcl`
- Grouping enables `terragrunt run --all plan` within a folder to scope operations

### Community Modules in Use

| Service | Source | Version |
|---|---|---|
| EKS | `terraform-aws-modules/eks/aws` | 21.19.0 |
| Karpenter (IAM/SQS) | `terraform-aws-modules/eks/aws//modules/karpenter` | 21.19.0 |
| EMR Virtual Cluster | `terraform-aws-modules/emr/aws//modules/virtual-cluster` | 3.3.0 |
| ECR | `terraform-aws-modules/ecr/aws` | 3.2.0 |
| S3 (per-bucket) | `terraform-aws-modules/s3-bucket/aws` | 5.12.0 |
| IAM Policy | `terraform-aws-modules/iam/aws//modules/iam-policy` | 6.6.0 |
| IAM IRSA Role | `terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts` | 6.6.0 |

### Local Wrapper Modules

| Module | Why local wrapper? |
|---|---|
| `modules/vpc/` | Simple VPC, no community module needed |
| `modules/ecr/` | Wraps community ECR module 4× via `for_each` into one composable unit |
| `modules/glue/` | Single `aws_glue_catalog_database` with `for_each` — simpler than cloudposse module with its label boilerplate |
