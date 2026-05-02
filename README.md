# lakehouse-at-scale

A data lakehouse platform integrating dbt with Dagster on AWS. Each dbt model maps 1:1 to a Dagster asset, executed on Apache Spark via EMR on EKS (de-team) or Amazon Athena (sales-team). Data is stored as Apache Iceberg tables on S3 with Glue Data Catalog as the metastore.

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Dagster, dagster-dbt, dagster-aws (Pipes) |
| Compute | Apache Spark (EMR on EKS), Amazon Athena |
| Data Transform | dbt-core, dbt-spark, dbt-athena |
| Table Format | Apache Iceberg + Glue Data Catalog |
| Infrastructure | Terraform + Terragrunt |
| GitOps | ArgoCD (App-of-Apps, Helm) |
| Node Scaling | Karpenter (Spot executors, On-Demand drivers) |
| CI/CD | GitHub Actions |
| Container Registry | Amazon ECR (Base Image + Code Image pattern) |

## Repository Structure

This is the **Infrastructure repo** — contains Terraform modules and Terragrunt configurations.

```
infra/
├── root.hcl                        # Root config: S3 remote state, native locking, AWS provider
├── _envcommon/                     # Shared Terragrunt configs
│   ├── vpc.hcl                     # ✅ VPC shared config
│   ├── eks.hcl                     # ✅ EKS (community module)
│   ├── karpenter.hcl               # ✅ Karpenter (community submodule)
│   ├── emr-virtual-cluster.hcl     # ✅ EMR Virtual Cluster (community submodule)
│   ├── ecr.hcl                     # ✅ ECR (local wrapper)
│   ├── s3.hcl                      # ✅ S3 shared config (community module)
│   ├── dagster-irsa-policy.hcl     # ✅ Dagster IAM policy (community module)
│   ├── dagster-irsa-role.hcl       # ✅ Dagster IRSA role (community module)
│   └── glue.hcl                    # ✅ Glue (local module)
├── modules/                        # Terraform modules (local only)
│   ├── vpc/                        # ✅ VPC module
│   ├── ecr/                        # ✅ ECR wrapper (4 repos via community module)
│   └── glue/                       # ✅ Glue module (Data Catalog databases)
└── non-prod/                       # Live environment
    └── ap-southeast-1/
        ├── env.hcl
        ├── vpc/                    # ✅ VPC
        ├── eks/                    # ✅ EKS
        ├── karpenter/              # ✅ Karpenter AWS resources
        ├── emr-virtual-cluster/    # ✅ EMR Virtual Cluster
        ├── ecr/                    # ✅ ECR repositories
        ├── s3/                     # ✅ S3 buckets (grouped)
        │   ├── data-lake/
        │   ├── pipes/
        │   └── spark-logs/
        ├── dagster-irsa/           # ✅ Dagster IAM (grouped)
        │   ├── de-team-policy/
        │   ├── de-team-role/
        │   ├── sales-team-policy/
        │   └── sales-team-role/
        └── glue/                   # ✅ Glue Data Catalog databases
```

## Modules

### ✅ VPC (`infra/modules/vpc/`)

VPC with public/private subnets (multi-AZ), NAT Gateway, Internet Gateway, and route tables. Subnets are tagged for Kubernetes ELB discovery.

| Output | Description |
|---|---|
| `vpc_id` | VPC ID |
| `private_subnet_ids` | Private subnet IDs (multi-AZ) |
| `public_subnet_ids` | Public subnet IDs (multi-AZ) |

### ✅ EKS (community module)

Uses [`terraform-aws-modules/eks/aws`](https://registry.terraform.io/modules/terraform-aws-modules/eks/aws/latest) v21.19.0 directly from Terragrunt (no local wrapper). EKS cluster with managed node group for system workloads, OIDC provider for IRSA, cluster addons (vpc-cni, coredns, kube-proxy, eks-pod-identity-agent), and node security group tagged for Karpenter discovery.

| Output | Description |
|---|---|
| `cluster_endpoint` | EKS cluster API server endpoint |
| `cluster_certificate_authority_data` | Base64 encoded cluster CA certificate |
| `cluster_name` | Name of the EKS cluster |
| `oidc_provider_arn` | ARN of the OIDC provider for IRSA |
| `node_security_group_id` | Node shared security group ID |

### ✅ Karpenter (community submodule — AWS resources only)

Uses [`terraform-aws-modules/eks/aws//modules/karpenter`](https://registry.terraform.io/modules/terraform-aws-modules/eks/aws/latest/submodules/karpenter) v21.19.0 directly from Terragrunt. Creates AWS-side resources only: IAM roles (controller + node), SQS queue, EventBridge rules, access entry. Karpenter Helm chart, NodePool CRDs, and EC2NodeClass are managed by ArgoCD.

| Output | Description |
|---|---|
| `iam_role_arn` | Karpenter controller IAM role ARN |
| `node_iam_role_arn` | Karpenter node IAM role ARN |
| `queue_name` | SQS queue name for interruption handling |

### ✅ EMR Virtual Cluster (community submodule)

Uses [`terraform-aws-modules/emr/aws//modules/virtual-cluster`](https://registry.terraform.io/modules/terraform-aws-modules/emr/aws/latest/submodules/virtual-cluster) v3.3.0 directly from Terragrunt (no local wrapper). Creates EMR on EKS Virtual Cluster, Kubernetes RBAC (Role, RoleBinding), IAM execution role, and CloudWatch log group. Namespace `spark` is managed by ArgoCD (`create_namespace = false`).

| Output | Description |
|---|---|
| `virtual_cluster_id` | EMR Virtual Cluster ID |
| `virtual_cluster_arn` | EMR Virtual Cluster ARN |
| `iam_role_arn` | EMR execution role ARN |

### ✅ ECR (`infra/modules/ecr/`)

Local wrapper module that calls [`terraform-aws-modules/ecr/aws`](https://registry.terraform.io/modules/terraform-aws-modules/ecr/aws/latest) v3.2.0 via `for_each` to create 4 ECR repositories: de-team-base, de-team-code, sales-team-base, sales-team-code. Each repo has lifecycle policy (keep 30 recent images) and image scanning on push.

| Output | Description |
|---|---|
| `repository_urls` | Map of repo name → repository URL |
| `repository_arns` | Map of repo name → repository ARN |

### ✅ S3 Buckets (community module — 3 Terragrunt units)

Uses [`terraform-aws-modules/s3-bucket/aws`](https://registry.terraform.io/modules/terraform-aws-modules/s3-bucket/aws/latest) v5.12.0 directly from Terragrunt. One shared envcommon (`s3.hcl`), each live config overrides `bucket` name. Grouped under `s3/` folder.

| Unit | Bucket | Purpose |
|---|---|---|
| `s3/data-lake/` | `lakehouse-at-scale-data-lake` | Iceberg warehouse storage |
| `s3/pipes/` | `lakehouse-at-scale-pipes` | Dagster Pipes S3 messages |
| `s3/spark-logs/` | `lakehouse-at-scale-spark-logs` | Spark job logs |

All buckets: versioning enabled, SSE-S3 encryption, all public access blocked.

### ✅ IAM Policies + IRSA Roles (community modules — 4 Terragrunt units)

Uses [`terraform-aws-modules/iam/aws//modules/iam-policy`](https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest) v6.6.0 and [`iam-role-for-service-accounts`](https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest) v6.6.0. Two shared envcommon files (`dagster-irsa-policy.hcl`, `dagster-irsa-role.hcl`), each live config provides team-specific inputs. Grouped under `dagster-irsa/` folder.

| Unit | Type | Purpose |
|---|---|---|
| `dagster-irsa/de-team-policy/` | IAM Policy | EMR job runs, S3 Pipes read/write, S3 logs read |
| `dagster-irsa/de-team-role/` | IRSA Role | Binds policy to `dagster:dagster-de-team` service account |
| `dagster-irsa/sales-team-policy/` | IAM Policy | Athena, S3 data lake read/write, Glue catalog |
| `dagster-irsa/sales-team-role/` | IRSA Role | Binds policy to `dagster:dagster-sales-team` service account |

### ✅ Glue (`infra/modules/glue/`)

Local module creating Glue Data Catalog databases for dbt schemas (staging, intermediate, marts) using `aws_glue_catalog_database` with `for_each`. Each database points to the S3 data lake bucket.

| Output | Description |
|---|---|
| `database_names` | Map of schema name → Glue database name |
| `catalog_id` | Glue Catalog ID (AWS account ID) |

### 🔲 Planned Modules

- **Docker Image** — Build & push Base Images

## Terragrunt Usage

```bash
# First run — bootstrap S3 backend bucket and plan all modules
cd infra/non-prod/ap-southeast-1
terragrunt run --all plan --backend-bootstrap

# Plan all modules
cd infra/non-prod/ap-southeast-1
terragrunt run --all plan

# Plan a single module
cd infra/non-prod/ap-southeast-1/vpc
terragrunt plan

# Apply a single module
cd infra/non-prod/ap-southeast-1/vpc
terragrunt apply

# Show dependency graph
cd infra/non-prod/ap-southeast-1
terragrunt graph-dependencies

# Destroy all resources (stop billing)
cd infra/non-prod/ap-southeast-1
terragrunt run --all destroy
```

## Related Repositories

| Repo | Contents |
|---|---|
| ArgoCD App Repo | Helm charts, K8s manifests (App-of-Apps pattern) |
| Application Code Repo | dbt projects + Dagster code (de-team, sales-team) |
