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
    └── ci-cd.yml                    # ✅ GitHub Actions CI/CD pipeline

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
│   ├── github-oidc-provider.hcl    # ✅ GitHub OIDC provider (community module)
│   ├── github-oidc-role.hcl        # ✅ GitHub OIDC role (community module)
│   └── glue.hcl                    # ✅ Glue (local module)
├── modules/                        # Terraform modules (local only)
│   ├── vpc/                        # ✅ VPC module
│   ├── ecr/                        # ✅ ECR wrapper (4 repos via community module)
│   ├── glue/                       # ✅ Glue module (Data Catalog databases)
│   └── docker-image/               # ✅ Build & push Base Images to ECR
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
        ├── github-oidc/            # ✅ GitHub Actions OIDC (grouped)
        │   ├── provider/
        │   └── role/
        └── glue/                   # ✅ Glue Data Catalog databases

argocd/                              # ✅ ArgoCD App-of-Apps (Helm charts + K8s manifests)
├── app-of-apps.yaml                 # ✅ Bootstrap Application CRD (kubectl apply once)
├── apps/                            # ✅ Root App-of-Apps Helm chart
│   ├── Chart.yaml
│   ├── values.yaml                  #   Single source of truth for all applications
│   └── templates/
│       ├── applications.yaml        #   Loop over values → ArgoCD Application CRDs
│       └── project.yaml             #   ArgoCD AppProject
├── karpenter/                       # ✅ Karpenter umbrella Helm chart (sync-wave: 2)
│   ├── Chart.yaml                   #   Dependency: official Karpenter chart v1.1.1
│   ├── values.yaml
│   └── templates/
│       ├── ec2nodeclass.yaml        #   EC2NodeClass (AL2023, tag-based discovery)
│       ├── nodepool-spark-drivers.yaml    # On-Demand (m5.large, m6i.large)
│       └── nodepool-spark-executors.yaml  # Spot (m5.xlarge/2xlarge, m6i.xlarge/2xlarge)
├── dagster/                         # ✅ Dagster umbrella Helm chart (sync-wave: 3)
│   ├── Chart.yaml                   #   Dependency: official Dagster chart v1.9.6
│   └── values.yaml                  #   2 code locations: de-team, sales-team
└── namespaces/                      # ✅ Namespace manifests (sync-wave: 1)
    ├── dagster-ns.yaml
    └── spark-ns.yaml

dbt-dagster-project/                     # ✅ dbt Projects + Dagster Application Code
├── de-team/
│   ├── Dockerfile.base
│   ├── Dockerfile.code
│   ├── dagster_project/                 # ✅ Dagster code location (de-team)
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
│   │   └── entrypoint.py               # ✅ Spark entrypoint (dbt build via Pipes)
│   └── dbt_project/                     # ✅ dbt-spark + Iceberg
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
    ├── dagster_project/                 # ✅ Dagster code location (sales-team)
    │   ├── __init__.py
    │   ├── definitions.py              #   Dagster Definitions entry point
    │   ├── assets/
    │   │   ├── __init__.py
    │   │   ├── dbt_assets.py           #   @dbt_assets → DbtCliResource (Athena)
    │   │   └── python_assets.py        #   Python-only assets (no Athena)
    │   └── resources/
    │       └── __init__.py             #   DbtCliResource factory
    └── dbt_project/                     # ✅ dbt-athena + Iceberg
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

### ✅ GitHub Actions OIDC (community modules — 2 Terragrunt units)

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

### ✅ Glue (`infra/modules/glue/`)

Local module creating Glue Data Catalog databases for dbt schemas (staging, intermediate, marts) using `aws_glue_catalog_database` with `for_each`. Each database points to the S3 data lake bucket.

| Output | Description |
|---|---|
| `database_names` | Map of schema name → Glue database name |
| `catalog_id` | Glue Catalog ID (AWS account ID) |

### ✅ Docker Image (`infra/modules/docker-image/`)

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

### ✅ ArgoCD App-of-Apps (`argocd/`)

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

### ✅ dbt Projects (Application Code Repo)

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

### ✅ Dagster Application Code — sales-team (`dbt-dagster-project/sales-team/dagster_project/`)

Complete Dagster code location for the sales-team, using dbt-athena assets that run dbt directly on the Dagster user code pod via `DbtCliResource`. Queries execute on Amazon Athena — no Spark/EMR needed.

| Component | File | Description |
|---|---|---|
| dbt Assets | `assets/dbt_assets.py` | `@dbt_assets` decorator, runs dbt via `DbtCliResource` (Athena) |
| Python Assets | `assets/python_assets.py` | Lightweight assets running on Dagster pod |
| Resources | `resources/__init__.py` | `DbtCliResource` factory for dbt-athena |
| Definitions | `definitions.py` | Entry point registering all assets and resources |

### ✅ CI/CD Pipeline (`.github/workflows/ci-cd.yml`)

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

### ✅ Dagster Application Code — de-team (`dbt-dagster-project/de-team/dagster_project/`)

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

## Docker Images (Local Testing Only)

Base Images are built and pushed to ECR by the Terraform `docker-image` module (`terragrunt apply` in `infra/non-prod/ap-southeast-1/docker-image/`). Code Images are built by the CI/CD pipeline on push to main. The commands below are only needed for local testing before pushing.

```bash
# Build de-team base image locally
cd dbt-dagster-project/de-team
docker build -f Dockerfile.base -t de-team-base:latest .

# Build de-team code image locally
docker build -f Dockerfile.code --build-arg BASE_IMAGE=de-team-base:latest -t de-team-code:latest .

# Build sales-team base image locally
cd dbt-dagster-project/sales-team
docker build -f Dockerfile.base -t sales-team-base:latest .

# Build sales-team code image locally
docker build -f Dockerfile.code --build-arg BASE_IMAGE=sales-team-base:latest -t sales-team-code:latest .
```

## Smoke Testing de-team Images

Two scripts help verify the de-team Base + Code image before (or without) going through the full Dagster → EMR run path. Use these when iterating on `Dockerfile.base`, Python dependencies, or Spark configuration.

### Local build + import check (`scripts/smoke-test-de-team-image.local.sh`)

Builds the Base Image and runs a quick `import` test using `python3.11` (matching EMR on EKS 7.13's `PYSPARK_PYTHON`). No ECR push, no AWS calls.

```bash
# Default tag (de-team-base:local-smoke)
./scripts/smoke-test-de-team-image.local.sh

# Custom tag
./scripts/smoke-test-de-team-image.local.sh my-tag
```

Use this as the fast feedback loop when changing `Dockerfile.base` or pinned package versions. Script runs in Git Bash / WSL / Linux.

### Remote EMR on EKS job submit (`scripts/smoke-test-de-team-image.ps1`)

Submits a minimal Spark job to the existing EMR Virtual Cluster using a Code Image tag already in ECR. Verifies end-to-end that the Spark driver can:

- Start with Iceberg extensions from `--jars`
- Import `dagster_pipes`, `dbt`, `boto3` in python3.11
- Create and stop a `SparkSession`

Does not build or push images — use the local script (or CI) first. Does not touch the running Dagster deployment.

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

The script uploads an inline smoke script to `s3://lakehouse-at-scale-pipes/smoke-tests/`, submits the EMR job, then polls until the job reaches a terminal state. `COMPLETED` = pass; on `FAILED` it prints `failureReason` + `stateDetails`.

### Remote dbt build on EMR on EKS (`scripts/smoke-test-dbt-model.ps1`)

Submits a Spark job that actually runs `dbt build --select <model>` using a Code Image already in ECR. The driver uses a debug runner uploaded to S3 (not the baked-in `/app/entrypoint.py`) and ships logs to CloudWatch + S3 — so Dagster Pipes is bypassed entirely and you see full dbt output (stdout, `run_results.json`, `dbt.log` tail).

The runner also:

- Writes a fresh `profiles.yml` to `/tmp` so you don't have to rebuild the Code Image to test profile changes.
- Redirects dbt `target/` and `logs/` to `/tmp` (EMR driver pods have a read-only root filesystem).

Use this when:

- Validating a change to `entrypoint.py`, `profiles.yml`, Dockerfile, or a dbt model without round-tripping through Dagster.
- Reproducing a production dbt error locally with the exact same image + IAM + Glue catalog as Dagster uses.

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

When the job fails, the script prints the CloudWatch log group/prefix and an `aws s3 cp` one-liner to fetch the driver stdout (where the `[smoke] ...` lines and the full dbt error live) once EMR syncs logs.

## CI/CD (GitHub Actions)

The CI/CD pipeline (`dbt-dagster-project/.github/workflows/ci-cd.yml`) requires 4 GitHub repository secrets. Here's how to obtain each one:

| Secret | How to obtain |
|---|---|
| `AWS_ACCOUNT_ID` | Your AWS account ID (`560503716668` for non-prod). Find it in the AWS Console top-right corner, or run `aws sts get-caller-identity --query Account --output text --profile non-prod` |
| `AWS_OIDC_ROLE_ARN` | Managed by Terragrunt: `cd infra/non-prod/ap-southeast-1/github-oidc && terragrunt run --all apply`, then `cd role && terragrunt output arn`. This creates the OIDC provider + IAM role with ECR push permissions, restricted to main branch |
| `ARGOCD_REPO` | The `owner/repo` path of your ArgoCD App Repo on GitHub (e.g. `thanhtt-demo/argocd-app-repo`). This is the repo that contains `argocd/dagster/values.yaml` where image tags are updated |
| `ARGOCD_REPO_PAT` | A GitHub Personal Access Token (classic or fine-grained) with `contents: write` permission on the ArgoCD App Repo. Create one at GitHub → Settings → Developer settings → Personal access tokens. The CI/CD pipeline uses this to push image tag updates to the ArgoCD repo |

To add these secrets: GitHub repo → Settings → Secrets and variables → Actions → New repository secret.

## ArgoCD

After Terraform modules are applied, populate ArgoCD Helm values with actual infrastructure outputs:

```bash
# Populate placeholders in ArgoCD values from Terraform outputs (creates a PR)
# Run from repo root in PowerShell:
powershell -ExecutionPolicy Bypass -File scripts/populate-argocd-values.ps1
```

Then access ArgoCD and bootstrap:

```bash
Set-Alias -Name k -Value kubectl

# Bootstrap — apply once to deploy everything
kubectl apply -f argocd/app-of-apps.yaml

# Validate App-of-Apps root chart
helm template argocd/apps/

# Validate Karpenter chart (requires helm dependency build first)
helm dependency build argocd/karpenter/
helm template argocd/karpenter/

# Validate Dagster chart (requires helm dependency build first)
helm dependency build argocd/dagster/
helm template argocd/dagster/
```

## Cleanup (Destroy All Resources)

To destroy all resources and stop billing:

```bash
# 1. Remove ArgoCD-managed K8s resources
kubectl delete -f argocd/app-of-apps.yaml --ignore-not-found
kubectl delete namespace dagster --ignore-not-found
kubectl delete namespace spark --ignore-not-found
kubectl delete nodeclaim --all --ignore-not-found
kubectl delete nodepool --all --ignore-not-found

# 2. Terminate Karpenter EC2 instances (orphan ENIs block EKS destroy)
aws ec2 describe-instances --filters "Name=tag:karpenter.sh/discovery,Values=lakehouse-at-scale-eks" "Name=instance-state-name,Values=running,stopped" --query "Reservations[].Instances[].InstanceId" --output text --profile non-prod --region ap-southeast-1
# Then: aws ec2 terminate-instances --instance-ids <IDs> --profile non-prod --region ap-southeast-1

# 3. Empty S3 buckets (versioning enabled, so need --force to delete all versions)
aws s3 rb s3://lakehouse-at-scale-data-lake --force --profile non-prod
aws s3 rb s3://lakehouse-at-scale-pipes --force --profile non-prod
aws s3 rb s3://lakehouse-at-scale-spark-logs --force --profile non-prod

# 4. Destroy all Terraform/Terragrunt resources (auto-resolves dependency order)
cd infra/non-prod/ap-southeast-1
terragrunt run --all destroy

# 5. (Optional) Delete ECR images to avoid storage costs
aws ecr batch-delete-image --repository-name lakehouse-at-scale/de-team-base --image-ids imageTag=latest --profile non-prod --region ap-southeast-1
aws ecr batch-delete-image --repository-name lakehouse-at-scale/de-team-code --image-ids imageTag=latest --profile non-prod --region ap-southeast-1
aws ecr batch-delete-image --repository-name lakehouse-at-scale/sales-team-base --image-ids imageTag=latest --profile non-prod --region ap-southeast-1
aws ecr batch-delete-image --repository-name lakehouse-at-scale/sales-team-code --image-ids imageTag=latest --profile non-prod --region ap-southeast-1
```

If `terragrunt run --all destroy` gets stuck on EKS, ensure all K8s namespaces and node claims are deleted first. EKS cluster deletion requires all managed node groups and Fargate profiles to be removed, which Terraform handles automatically.

## Related Repositories

| Repo | Contents |
|---|---|
| ArgoCD App Repo | Helm charts, K8s manifests (App-of-Apps pattern) |
| Application Code Repo | dbt projects + Dagster code (de-team, sales-team) |
