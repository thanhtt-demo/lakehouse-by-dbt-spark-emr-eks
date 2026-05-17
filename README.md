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

This is the **Infrastructure repo** — contains Terraform modules, Terragrunt configurations, and ArgoCD Helm charts.

```
.github/
└── workflows/
    └── ci-cd.yml                    #  GitHub Actions CI/CD pipeline

infra/
├── root.hcl                        # Root config: S3 remote state, native locking, AWS provider
├── _envcommon/                     # Shared Terragrunt configs
│   ├── vpc.hcl                     #  VPC shared config
│   ├── eks.hcl                     #  EKS (community module)
│   ├── karpenter.hcl               #  Karpenter (community submodule)
│   ├── emr-virtual-cluster.hcl     #  EMR Virtual Cluster (community submodule)
│   ├── ecr.hcl                     #  ECR (local wrapper)
│   ├── s3.hcl                      #  S3 shared config (community module)
│   ├── dagster-irsa-policy.hcl     #  Dagster IAM policy (community module)
│   ├── dagster-irsa-role.hcl       #  Dagster IRSA role (community module)
│   ├── github-oidc-provider.hcl    #  GitHub OIDC provider (community module)
│   ├── github-oidc-role.hcl        #  GitHub OIDC role (community module)
│   └── glue.hcl                    #  Glue (local module)
├── modules/                        # Terraform modules (local only)
│   ├── vpc/                        #  VPC module
│   ├── ecr/                        #  ECR wrapper (4 repos via community module)
│   ├── glue/                       #  Glue module (Data Catalog databases)
│   └── docker-image/               #  Build & push Base Images to ECR
└── non-prod/                       # Live environment
    └── ap-southeast-1/
        ├── env.hcl
        ├── vpc/                    #  VPC
        ├── eks/                    #  EKS
        ├── karpenter/              #  Karpenter AWS resources
        ├── emr-virtual-cluster/    #  EMR Virtual Cluster
        ├── ecr/                    #  ECR repositories
        ├── s3/                     #  S3 buckets (grouped)
        │   ├── data-lake/
        │   ├── pipes/
        │   └── spark-logs/
        ├── dagster-irsa/           #  Dagster IAM (grouped)
        │   ├── de-team-policy/
        │   ├── de-team-role/
        │   ├── sales-team-policy/
        │   └── sales-team-role/
        ├── github-oidc/            #  GitHub Actions OIDC (grouped)
        │   ├── provider/
        │   └── role/
        └── glue/                   #  Glue Data Catalog databases

argocd/                              #  ArgoCD App-of-Apps (Helm charts + K8s manifests)
├── app-of-apps.yaml                 #  Bootstrap Application CRD (kubectl apply once)
├── apps/                            #  Root App-of-Apps Helm chart
│   ├── Chart.yaml
│   ├── values.yaml                  #   Single source of truth for all applications
│   └── templates/
│       ├── applications.yaml        #   Loop over values → ArgoCD Application CRDs
│       └── project.yaml             #   ArgoCD AppProject
├── karpenter/                       #  Karpenter umbrella Helm chart (sync-wave: 2)
│   ├── Chart.yaml                   #   Dependency: official Karpenter chart v1.1.1
│   ├── values.yaml
│   └── templates/
│       ├── ec2nodeclass.yaml        #   EC2NodeClass (AL2023, tag-based discovery)
│       ├── nodepool-spark-drivers.yaml    # On-Demand (m5.large, m6i.large)
│       └── nodepool-spark-executors.yaml  # Spot (m5.xlarge/2xlarge, m6i.xlarge/2xlarge)
├── dagster/                         #  Dagster umbrella Helm chart (sync-wave: 3)
│   ├── Chart.yaml                   #   Dependency: official Dagster chart v1.9.6
│   └── values.yaml                  #   2 code locations: de-team, sales-team
└── namespaces/                      #  Namespace manifests (sync-wave: 1)
    ├── dagster-ns.yaml
    └── spark-ns.yaml

dbt-dagster-project/                     #  dbt Projects + Dagster Application Code
├── de-team/
│   ├── Dockerfile.base
│   ├── Dockerfile.code
│   ├── dagster_project/                 #  Dagster code location (de-team)
│   │   ├── __init__.py
│   │   ├── definitions.py              #   Dagster Definitions entry point
│   │   ├── assets/
│   │   │   ├── __init__.py
│   │   │   ├── dbt_assets.py           #   @dbt_assets → EMR on EKS via Pipes
│   │   │   └── python_assets.py        #   Python-only assets (no Spark)
│   │   ├── resources/
│   │   │   └── __init__.py             #   PipesEMRContainersClient factory
│   │   └── utils/
│   │       ├── __init__.py
│   │       └── spark_config.py         #   SparkConfigManager (merge + params builder)
│   ├── spark_entrypoint/
│   │   └── entrypoint.py               #  Spark entrypoint (dbt build via Pipes)
│   └── dbt_project/                     #  dbt-spark + Iceberg
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── macros/
│       │   └── generate_schema_name.sql
│       └── models/
│           ├── staging/
│           │   ├── stg_raw_orders.sql
│           │   └── schema.yml
│           ├── intermediate/
│           └── marts/
│               ├── orders.sql           # Incremental merge, spark_config meta
│               └── orders.yml
└── sales-team/
    ├── Dockerfile.base
    ├── Dockerfile.code
    ├── dagster_project/                 #  Dagster code location (sales-team)
    │   ├── __init__.py
    │   ├── definitions.py              #   Dagster Definitions entry point
    │   ├── assets/
    │   │   ├── __init__.py
    │   │   ├── dbt_assets.py           #   @dbt_assets → DbtCliResource (Athena)
    │   │   └── python_assets.py        #   Python-only assets (no Athena)
    │   └── resources/
    │       └── __init__.py             #   DbtCliResource factory
    └── dbt_project/                     #  dbt-athena + Iceberg
        ├── dbt_project.yml
        ├── profiles.yml
        ├── macros/
        │   └── generate_schema_name.sql
        └── models/
            ├── staging/
            │   ├── stg_sales.sql
            │   └── schema.yml
            └── marts/
```

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Terraform | 1.15.1 | `terraform -version` |
| Terragrunt | 1.0.3 | `terragrunt --version` |
| AWS CLI v2 | 2.17.0 | `aws --version` |
| kubectl | 1.29.2 | `kubectl version --client` |
| Helm | 3.17.0 | `helm version` |
| Docker | 26.0 | `docker --version` |

### AWS IAM User / Role

You need an IAM user or role with permissions to create: VPC, EKS, EMR, S3, ECR, IAM, Glue, SQS, EventBridge. The simplest approach for non-prod/demo is `AdministratorAccess`.

### AWS CLI Profile

Configure profile name `non-prod` (matching `env.hcl`):

```bash
aws configure --profile non-prod
# AWS Access Key ID: <your-key>
# AWS Secret Access Key: <your-secret>
# Default region: ap-southeast-1
# Default output format: json
```

Verify the profile works:

```bash
aws sts get-caller-identity --profile non-prod
```

## How to Run

> **Note:** For simplicity debugging, this guide uses a local PC or EC2 instance as the deployment server for Terragrunt code. In production, use a CI/CD pipeline (GitHub Actions) or a dedicated deployment server.

### 1. Fork & Configure Repository

This repo contains hardcoded references to `thanhtt-demo/lakehouse-by-dbt-spark-emr-eks`. Before deploying:

1. Fork or clone this repo to your own GitHub account
2. Update the following files with your repo URL:
   - `infra/_envcommon/argocd.hcl` → `argocd_repo_url`
   - `argocd/app-of-apps.yaml` → `spec.source.repoURL`
   - `argocd/apps/values.yaml` → `spec.source.repoURL` and `appProject.sourceRepos`
3. Push to your repo's `main` branch

### 2. Provision Infrastructure (Terragrunt)

```bash
# Bootstrap S3 backend + plan all modules
cd infra/non-prod/ap-southeast-1
terragrunt run --all plan --backend-bootstrap

# Apply all modules (dependency order resolved automatically)
terragrunt run --all apply
```

This step takes approximately **~30 minutes** to deploy all resources:

| Module | Resource | Est. Time |
|---|---|---|
| `vpc` | VPC, Subnets, NAT Gateway, Route Tables | ~3 min |
| `eks` | EKS Cluster, Managed Node Group, OIDC Provider, Addons | ~15 min |
| `karpenter` | IAM Roles, SQS Queue, EventBridge Rules | ~2 min |
| `s3/*` | 3 S3 Buckets (data-lake, pipes, spark-logs) | ~1 min |
| `ecr` | 4 ECR Repositories | ~1 min |
| `emr-virtual-cluster` | EMR Virtual Cluster, RBAC, IAM Execution Role | ~2 min |
| `dagster-irsa/*` | 2 IAM Policies + 2 IRSA Roles | ~2 min |
| `github-oidc/*` | OIDC Provider + IAM Role | ~1 min |
| `glue` | Glue Data Catalog Databases | ~1 min |
| `docker-image` | Build & push Base Images to ECR | ~5 min |

### 3. Configure kubectl

```bash
aws eks update-kubeconfig --name lakehouse-at-scale-eks --region ap-southeast-1 --profile non-prod
kubectl get nodes
```

### 4. Bootstrap App-of-Apps

ArgoCD is installed automatically by Terraform (`infra/non-prod/ap-southeast-1/argocd/`). It deploys ArgoCD via Helm and creates the App-of-Apps Application CRD — no manual install needed.

If you need to update ArgoCD Helm values with Terraform outputs (e.g. after EKS recreation):

```bash
# Populate placeholders in ArgoCD values from Terraform outputs (PowerShell)
powershell -ExecutionPolicy Bypass -File scripts/populate-argocd-values.ps1
```

### 5. Configure GitHub Actions Secrets

After infrastructure is deployed, configure CI/CD secrets in your GitHub repo (Settings → Secrets → Actions):

```bash
# Get the OIDC role ARN from Terraform output
cd infra/non-prod/ap-southeast-1/github-oidc/role
terragrunt output arn
```

| Secret | Value |
|---|---|
| `AWS_ACCOUNT_ID` | Your AWS account ID (e.g. `560503716668`) |
| `AWS_OIDC_ROLE_ARN` | Output from `terragrunt output arn` above |
| `ARGOCD_REPO` | Your repo path (e.g. `your-org/lakehouse-by-dbt-spark-emr-eks`) |
| `ARGOCD_REPO_PAT` | GitHub PAT with `contents: write` on the repo |

### 6. Access UIs (Port Forward)

> **Note:** Using port-forward to save ALB costs. In production, configure ALB Ingress Controller + domain instead.

```bash
# ArgoCD UI — https://localhost:8080
kubectl port-forward svc/argocd-server -n argocd 8080:443

# Get ArgoCD admin password
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d

# Dagster UI — http://localhost:3000
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80
```
### 6.2 Create raw table for test(athena query)

```sql
CREATE TABLE raw.raw_sales (
	sale_id STRING,
	product_id STRING,
	customer_id STRING,
	sale_date DATE,
	quantity INT,
	unit_price DOUBLE,
	total_amount DOUBLE,
	region STRING,
	updated_at TIMESTAMP
)
LOCATION 's3://lakehouse-at-scale-data-lake/warehouse/raw/raw_sales/'
TBLPROPERTIES ('table_type' = 'ICEBERG')

-- Insert sample data
INSERT INTO raw.raw_sales VALUES
('S001', 'P001', 'C001', DATE '2026-01-15', 10, 29.99, 299.90, 'APAC', TIMESTAMP '2026-01-15 10:00:00'),
('S002', 'P002', 'C002', DATE '2026-01-16', 5, 49.99, 249.95, 'EMEA', TIMESTAMP '2026-01-16 11:00:00'),
('S003', 'P001', 'C003', DATE '2026-01-17', 3, 29.99, 89.97, 'NA', TIMESTAMP '2026-01-17 09:00:00')


CREATE TABLE raw.raw_orders (
    order_id     STRING,
    customer_id  STRING,
    order_date   DATE,
    status       STRING,
    amount       DOUBLE,
    currency     STRING,
    updated_at   TIMESTAMP
)
LOCATION 's3://lakehouse-at-scale-data-lake/warehouse/raw/raw_orders/'
TBLPROPERTIES ('table_type' = 'ICEBERG')

INSERT INTO raw.raw_orders VALUES
('O001', 'C001', DATE '2026-01-15', 'completed', 299.90, 'USD', TIMESTAMP '2026-01-15 10:00:00'),
('O002', 'C002', DATE '2026-01-16', 'completed', 249.95, 'USD', TIMESTAMP '2026-01-16 11:00:00'),
('O003', 'C003', DATE '2026-01-17', 'pending',    89.97, 'USD', TIMESTAMP '2026-01-17 09:00:00'),
('O004', 'C001', DATE '2026-01-18', 'cancelled', 150.00, 'USD', TIMESTAMP '2026-01-18 12:30:00'),
('O005', 'C004', DATE '2026-01-19', 'completed', 420.50, 'USD', TIMESTAMP '2026-01-19 14:15:00');
```

```sql
-- Create Iceberg table raw_customers in Glue Data Catalog (raw database)
CREATE TABLE raw.raw_customers (
    customer_id     STRING,
    customer_name   STRING,
    email           STRING,
    region          STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
)
USING iceberg
LOCATION 's3://lakehouse-at-scale-data-lake/raw/raw_customers/';

-- Insert sample data
INSERT INTO raw.raw_customers VALUES
    ('C001', 'Nguyen Van A',  'nguyenvana@example.com',  'APAC', TIMESTAMP '2024-01-15 08:00:00', TIMESTAMP '2024-06-01 10:30:00'),
    ('C002', 'Tran Thi B',    'tranthib@example.com',    'APAC', TIMESTAMP '2024-02-20 09:15:00', TIMESTAMP '2024-07-10 14:00:00'),
    ('C003', 'Le Van C',      'levanc@example.com',      'APAC', TIMESTAMP '2024-03-10 11:00:00', TIMESTAMP '2024-08-05 16:45:00'),
    ('C004', 'John Smith',    'john.smith@example.com',  'NA',   TIMESTAMP '2024-01-05 07:30:00', TIMESTAMP '2024-09-12 09:00:00'),
    ('C005', 'Emma Johnson',  'emma.j@example.com',      'NA',   TIMESTAMP '2024-04-18 13:00:00', TIMESTAMP '2024-10-01 11:20:00'),
    ('C006', 'Hans Mueller',  'hans.m@example.com',      'EMEA', TIMESTAMP '2024-05-22 10:45:00', TIMESTAMP '2024-10-15 08:30:00'),
    ('C007', 'Pham Thi D',    'phamthid@example.com',    'APAC', TIMESTAMP '2024-06-01 14:30:00', TIMESTAMP '2024-11-01 17:00:00'),
    ('C008', 'Maria Garcia',  'maria.g@example.com',     'EMEA', TIMESTAMP '2024-07-12 09:00:00', TIMESTAMP '2024-11-20 12:15:00');
```


### 7. Trigger a dbt Run

Once the Dagster UI is accessible, go to Assets → Materialize all to trigger the first dbt run. Dagster will submit a Spark job via EMR on EKS (de-team) or query Athena (sales-team).

### 8. Cleanup (Stop Billing)

```bash
# 1. Remove ArgoCD-managed K8s resources
kubectl delete -f argocd/app-of-apps.yaml --ignore-not-found
kubectl delete namespace dagster spark --ignore-not-found
kubectl delete nodeclaim,nodepool --all --ignore-not-found

# 2. Empty S3 buckets (versioning enabled, --force deletes all versions)
aws s3 rb s3://lakehouse-at-scale-data-lake --force --profile non-prod
aws s3 rb s3://lakehouse-at-scale-pipes --force --profile non-prod
aws s3 rb s3://lakehouse-at-scale-spark-logs --force --profile non-prod

# 3. Destroy all Terraform/Terragrunt resources (auto-resolves dependency order)
cd infra/non-prod/ap-southeast-1
terragrunt run --all destroy
```

## Modules

###  VPC (`infra/modules/vpc/`)

VPC with public/private subnets (multi-AZ), NAT Gateway, Internet Gateway, and route tables. Subnets are tagged for Kubernetes ELB discovery.

| Output | Description |
|---|---|
| `vpc_id` | VPC ID |
| `private_subnet_ids` | Private subnet IDs (multi-AZ) |
| `public_subnet_ids` | Public subnet IDs (multi-AZ) |

###  EKS (community module)

Uses [`terraform-aws-modules/eks/aws`](https://registry.terraform.io/modules/terraform-aws-modules/eks/aws/latest) v21.19.0 directly from Terragrunt (no local wrapper). EKS cluster with managed node group for system workloads, OIDC provider for IRSA, cluster addons (vpc-cni, coredns, kube-proxy, eks-pod-identity-agent), and node security group tagged for Karpenter discovery.

| Output | Description |
|---|---|
| `cluster_endpoint` | EKS cluster API server endpoint |
| `cluster_certificate_authority_data` | Base64 encoded cluster CA certificate |
| `cluster_name` | Name of the EKS cluster |
| `oidc_provider_arn` | ARN of the OIDC provider for IRSA |
| `node_security_group_id` | Node shared security group ID |

###  Karpenter (community submodule — AWS resources only)

Uses [`terraform-aws-modules/eks/aws//modules/karpenter`](https://registry.terraform.io/modules/terraform-aws-modules/eks/aws/latest/submodules/karpenter) v21.19.0 directly from Terragrunt. Creates AWS-side resources only: IAM roles (controller + node), SQS queue, EventBridge rules, access entry. Karpenter Helm chart, NodePool CRDs, and EC2NodeClass are managed by ArgoCD.

| Output | Description |
|---|---|
| `iam_role_arn` | Karpenter controller IAM role ARN |
| `node_iam_role_arn` | Karpenter node IAM role ARN |
| `queue_name` | SQS queue name for interruption handling |

###  EMR Virtual Cluster (community submodule)

Uses [`terraform-aws-modules/emr/aws//modules/virtual-cluster`](https://registry.terraform.io/modules/terraform-aws-modules/emr/aws/latest/submodules/virtual-cluster) v3.3.0 directly from Terragrunt (no local wrapper). Creates EMR on EKS Virtual Cluster, Kubernetes RBAC (Role, RoleBinding), IAM execution role, and CloudWatch log group. Namespace `spark` is managed by ArgoCD (`create_namespace = false`).

| Output | Description |
|---|---|
| `virtual_cluster_id` | EMR Virtual Cluster ID |
| `virtual_cluster_arn` | EMR Virtual Cluster ARN |
| `iam_role_arn` | EMR execution role ARN |

###  ECR (`infra/modules/ecr/`)

Local wrapper module that calls [`terraform-aws-modules/ecr/aws`](https://registry.terraform.io/modules/terraform-aws-modules/ecr/aws/latest) v3.2.0 via `for_each` to create 4 ECR repositories: de-team-base, de-team-code, sales-team-base, sales-team-code. Each repo has lifecycle policy (keep 30 recent images) and image scanning on push.

| Output | Description |
|---|---|
| `repository_urls` | Map of repo name → repository URL |
| `repository_arns` | Map of repo name → repository ARN |

###  S3 Buckets (community module — 3 Terragrunt units)

Uses [`terraform-aws-modules/s3-bucket/aws`](https://registry.terraform.io/modules/terraform-aws-modules/s3-bucket/aws/latest) v5.12.0 directly from Terragrunt. One shared envcommon (`s3.hcl`), each live config overrides `bucket` name. Grouped under `s3/` folder.

| Unit | Bucket | Purpose |
|---|---|---|
| `s3/data-lake/` | `lakehouse-at-scale-data-lake` | Iceberg warehouse storage |
| `s3/pipes/` | `lakehouse-at-scale-pipes` | Dagster Pipes S3 messages |
| `s3/spark-logs/` | `lakehouse-at-scale-spark-logs` | Spark job logs |

All buckets: versioning enabled, SSE-S3 encryption, all public access blocked.

###  IAM Policies + IRSA Roles (community modules — 4 Terragrunt units)

Uses [`terraform-aws-modules/iam/aws//modules/iam-policy`](https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest) v6.6.0 and [`iam-role-for-service-accounts`](https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest) v6.6.0. Two shared envcommon files (`dagster-irsa-policy.hcl`, `dagster-irsa-role.hcl`), each live config provides team-specific inputs. Grouped under `dagster-irsa/` folder.

| Unit | Type | Purpose |
|---|---|---|
| `dagster-irsa/de-team-policy/` | IAM Policy | EMR job runs, S3 Pipes read/write, S3 logs read |
| `dagster-irsa/de-team-role/` | IRSA Role | Binds policy to `dagster:dagster-de-team` service account |
| `dagster-irsa/sales-team-policy/` | IAM Policy | Athena, S3 data lake read/write, Glue catalog |
| `dagster-irsa/sales-team-role/` | IRSA Role | Binds policy to `dagster:dagster-sales-team` service account |

###  GitHub Actions OIDC (community modules — 2 Terragrunt units)

Uses [`terraform-aws-modules/iam/aws//modules/iam-oidc-provider`](https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest/submodules/iam-oidc-provider) and [`iam-role`](https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest/submodules/iam-role) v6.6.0 with `enable_github_oidc`. Creates the OIDC identity provider and an IAM role for GitHub Actions CI/CD pipeline with ECR push permissions. Grouped under `github-oidc/` folder.

| Unit | Type | Purpose |
|---|---|---|
| `github-oidc/provider/` | OIDC Provider | Trusts `token.actions.githubusercontent.com` (one-time per account) |
| `github-oidc/role/` | IAM Role | `lakehouse-at-scale-github-actions` — ECR push, restricted to main branch |

After `terragrunt apply`, the role ARN output is the value for the `AWS_OIDC_ROLE_ARN` GitHub secret:
```bash
cd infra/non-prod/ap-southeast-1/github-oidc/role
terragrunt output arn
```

###  Glue (`infra/modules/glue/`)

Local module creating Glue Data Catalog databases for dbt schemas (staging, intermediate, marts) using `aws_glue_catalog_database` with `for_each`. Each database points to the S3 data lake bucket.

| Output | Description |
|---|---|
| `database_names` | Map of schema name → Glue database name |
| `catalog_id` | Glue Catalog ID (AWS account ID) |

###  Docker Image (`infra/modules/docker-image/`)

Builds and pushes Base Images to ECR using `null_resource` + `local-exec` provisioner. Only rebuilds when the Dockerfile content changes (triggered by `filemd5` hash). Extracts ECR registry and AWS region from the repository URL automatically.

| Variable | Description |
|---|---|
| `dockerfile_path` | Path to the Dockerfile.base to build |
| `ecr_repository_url` | ECR repository URL to push to |
| `image_tag` | Tag for the Docker image (default: `latest`) |
| `docker_context` | Docker build context directory |

| Output | Description |
|---|---|
| `image_uri` | Full image URI (ecr_url:tag) |
| `dockerfile_hash` | MD5 hash of the Dockerfile for change detection |

###  ArgoCD App-of-Apps (`argocd/`)

Helm charts and Kubernetes manifests for ArgoCD GitOps deployment. Uses the App-of-Apps pattern with sync waves for ordered deployment.

| Component | Path | Sync Wave | Description |
|---|---|---|---|
| Bootstrap | `app-of-apps.yaml` | — | `kubectl apply -f` once to deploy everything |
| Root chart | `apps/` | — | Loops over `values.yaml` to create ArgoCD Application CRDs |
| Namespaces | `namespaces/` | 1 | `dagster` and `spark` namespace manifests |
| Karpenter | `karpenter/` | 2 | Official chart v1.1.1 + NodePool/EC2NodeClass CRDs |
| Dagster | `dagster/` | 3 | Official chart v1.9.6 + 2 user code deployments |

Karpenter NodePools:
- `spark-executors`: Spot (m5.xlarge/2xlarge, m6i.xlarge/2xlarge), taint `spark-role=executor:NoSchedule`, consolidateAfter 300s
- `spark-drivers`: On-Demand (m5.large, m6i.large), consolidateAfter 120s

Dagster user code deployments:
- `de-team`: dbt-spark, PipesEMRContainersClient, service account with IRSA
- `sales-team`: dbt-athena, service account with IRSA

###  dbt Projects (Application Code Repo)

Two dbt projects, one per team code location, both writing Iceberg tables to the same S3 Data Lake via Glue Data Catalog.

| Project | Adapter | Compute | Schema Mapping |
|---|---|---|---|
| `de-team/dbt_project/` | dbt-spark (`method: session`) | Spark on EMR on EKS | staging, intermediate, marts → Glue DBs |
| `sales-team/dbt_project/` | dbt-athena | Amazon Athena | staging, marts → Glue DBs |

Both projects use `generate_schema_name` macro to map dbt schemas directly to Glue Data Catalog database names. Default materialization is `table` with `file_format: iceberg`.

de-team sample models:
- `stg_raw_orders` (staging, table) → `orders` (marts, incremental merge with `unique_key: order_id`)
- `orders.yml` includes `meta.spark_config` for custom Spark resources (driver_cpu: 2, executor_instances: 4)

sales-team sample models:
- `stg_sales` (staging, table) — selects from raw sales source via Athena

###  Dagster Application Code — sales-team (`dbt-dagster-project/sales-team/dagster_project/`)

Complete Dagster code location for the sales-team, using dbt-athena assets that run dbt directly on the Dagster user code pod via `DbtCliResource`. Queries execute on Amazon Athena — no Spark/EMR needed.

| Component | File | Description |
|---|---|---|
| dbt Assets | `assets/dbt_assets.py` | `@dbt_assets` decorator, runs dbt via `DbtCliResource` (Athena) |
| Python Assets | `assets/python_assets.py` | Lightweight assets running on Dagster pod |
| Resources | `resources/__init__.py` | `DbtCliResource` factory for dbt-athena |
| Definitions | `definitions.py` | Entry point registering all assets and resources |

###  CI/CD Pipeline (`.github/workflows/ci-cd.yml`)

GitHub Actions workflow that builds Code Images and updates ArgoCD on push to main. Only rebuilds the code location(s) whose files changed (path filters via `dorny/paths-filter`).

| Job | Trigger | Description |
|---|---|---|
| `detect-changes` | Always | Determines which code locations have changed files |
| `build-de-team` | `de-team/**` changed | `dagster-dbt prepare-and-package` → build Code Image → push ECR (tag: git SHA) |
| `build-sales-team` | `sales-team/**` changed | Same flow for sales-team |
| `update-argocd` | Any build succeeded | Updates image tag in ArgoCD Dagster Helm values → push commit → ArgoCD auto-syncs |

Required GitHub secrets: `AWS_ACCOUNT_ID`, `AWS_OIDC_ROLE_ARN`, `ARGOCD_REPO`, `ARGOCD_REPO_PAT`.

### Docker Images (Application Code Repo)

Four Dockerfiles following the Base Image + Code Image pattern:

| File | Base | Contents |
|---|---|---|
| `de-team/Dockerfile.base` | `public.ecr.aws/emr-on-eks/spark/emr-7.13.0` | Spark + dbt-spark + dagster-pipes + Iceberg JAR |
| `de-team/Dockerfile.code` | `de-team-base:latest` | COPY dbt_project + spark_entrypoint + dagster_project |
| `sales-team/Dockerfile.base` | `python:3.10-slim` | dbt-athena + dagster + dagster-aws + dagster-dbt |
| `sales-team/Dockerfile.code` | `sales-team-base:latest` | COPY dbt_project + dagster_project |

###  Dagster Application Code — de-team (`dbt-dagster-project/de-team/dagster_project/`)

Complete Dagster code location for the de-team, including dbt-spark assets that submit Spark jobs to EMR on EKS via Dagster Pipes, Python-only assets, and SparkConfigManager for per-model Spark resource configuration.

| Component | File | Description |
|---|---|---|
| SparkConfigManager | `utils/spark_config.py` | Merges per-model Spark config with defaults, builds `start_job_run_params` |
| dbt Assets | `assets/dbt_assets.py` | `@dbt_assets` decorator, submits Spark jobs via `PipesEMRContainersClient` |
| Python Assets | `assets/python_assets.py` | Lightweight assets running on Dagster pod (no Spark) |
| Spark Entrypoint | `spark_entrypoint/entrypoint.py` | Runs `dbt build` inside Spark Driver Pod, reports via Pipes |
| Resources | `resources/__init__.py` | `PipesEMRContainersClient` factory with `PipesS3MessageReader` |
| Definitions | `definitions.py` | Entry point registering all assets and resources |

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