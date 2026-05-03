# Deployment Checklist

Conversation ID: `53a6e3a6-44ae-472b-b2cb-a7630f4e286b`

## Environment

| Key | Value |
|---|---|
| AWS Account | `560503716668` |
| AWS Profile | `non-prod` |
| AWS Region | `ap-southeast-1` |
| EKS Cluster Name | `lakehouse-at-scale-eks` |
| Kubernetes Version | `1.35` |

---

## Task 1 — VPC

### Verify

```bash
aws ec2 describe-vpcs \
  --filters "Name=tag:Name,Values=lakehouse-at-scale-vpc" \
  --query "Vpcs[0].{VpcId:VpcId,State:State,CidrBlock:CidrBlock}" \
  --profile non-prod --region ap-southeast-1
```

| Check | Expected |
|---|---|
| VPC state | `available` |
| CIDR | `10.0.0.0/16` |
| Private subnets | 3 (one per AZ) |
| Public subnets | 3 (one per AZ) |
| NAT Gateway | 1 (single NAT) |
| Internet Gateway | 1 |

### Status

- [ ] VPC verified

---

## Task 2 — EKS Cluster

### Verify

```bash
# Cluster status
aws eks describe-cluster \
  --name lakehouse-at-scale-eks \
  --query "cluster.{Status:status,Version:version,Endpoint:endpoint,PlatformVersion:platformVersion}" \
  --profile non-prod --region ap-southeast-1

# Update kubeconfig 
 aws eks update-kubeconfig --name lakehouse-at-scale-eks --profile non-prod --region ap-southeast-1


# Nodes
kubectl get nodes -o wide

# System pods
kubectl get pods -n kube-system

# Addons
aws eks list-addons --cluster-name lakehouse-at-scale-eks --profile non-prod --region ap-southeast-1
```

| Check | Expected |
|---|---|
| Cluster status | `ACTIVE` |
| Kubernetes version | `1.35` |
| System nodes | 2 nodes `Ready` |
| coredns pods | 2 replicas `Running` |
| vpc-cni pods | 1 per node `Running` |
| kube-proxy pods | 1 per node `Running` |
| eks-pod-identity-agent | 1 per node `Running` |
| OIDC provider | exists (for IRSA) |
| Node labels | `role=system`, `karpenter.sh/controller=true` |
| Node SG tag | `karpenter.sh/discovery=lakehouse-at-scale-eks` |

### Status

- [ ] EKS cluster verified
- [ ] kubeconfig updated
- [ ] Nodes ready
- [ ] Addons running

---

## Task 2 — Karpenter AWS Resources

### Verify

```bash
# SQS queue
aws sqs list-queues --queue-name-prefix Karpenter --profile non-prod --region ap-southeast-1

# Karpenter node IAM role
aws iam get-role \
  --role-name lakehouse-at-scale-karpenter-node \
  --query "Role.{RoleName:RoleName,Arn:Arn}" \
  --profile non-prod

# Karpenter controller IAM role
aws iam list-roles \
  --query "Roles[?contains(RoleName,'KarpenterController')].{RoleName:RoleName,Arn:Arn}" \
  --profile non-prod

# List pod associations
aws eks list-pod-identity-associations --cluster-name lakehouse-at-scale-eks --profile non-prod --region ap-southeast-1

# EventBridge rules
aws events list-rules --name-prefix Karpenter --query "Rules[].{Name:Name,State:State}" --profile non-prod --region ap-southeast-1

# EKS access entries
aws eks list-access-entries --cluster-name lakehouse-at-scale-eks --profile non-prod --region ap-southeast-1
```

| Check | Expected |
|---|---|
| SQS queue | exists, SSE enabled |
| Node IAM role | `lakehouse-at-scale-karpenter-node` exists |
| Controller IAM role | `KarpenterController-*` exists |
| EventBridge rules | Spot interruption + rebalance + state change |
| Access entry | Karpenter node role has access |

### Status

- [ ] SQS queue verified
- [ ] Node IAM role verified
- [ ] Controller IAM role verified
- [ ] Pod Identity association verified
- [ ] EventBridge rules verified
- [ ] Access entry verified

---

## Task 3 — EMR Virtual Cluster

### Verify

```bash
# EMR Virtual Cluster
aws emr-containers list-virtual-clusters \
  --query "virtualClusters[?name=='lakehouse-at-scale-emr-vc'].{Id:id,Name:name,State:state,EksCluster:containerProvider.id}" \
  --profile non-prod --region ap-southeast-1

# EMR execution IAM role
aws iam get-role \
  --role-name lakehouse-at-scale-emr-execution \
  --query "Role.{RoleName:RoleName,Arn:Arn}" \
  --profile non-prod

# CloudWatch log group
aws logs describe-log-groups \
  --log-group-name-prefix "/emr-on-eks/lakehouse-at-scale" \
  --query "logGroups[0].{Name:logGroupName,RetentionDays:retentionInDays}" \
  --profile non-prod --region ap-southeast-1

# Kubernetes RBAC (requires kubeconfig)
kubectl get role -n spark
kubectl get rolebinding -n spark
```

| Check | Expected |
|---|---|
| Virtual Cluster state | `RUNNING` |
| Container provider | `lakehouse-at-scale-eks` |
| Namespace | `spark` |
| Execution IAM role | `lakehouse-at-scale-emr-execution` exists |
| CloudWatch log group | `/emr-on-eks/lakehouse-at-scale` exists |
| K8s Role | EMR role in `spark` namespace |
| K8s RoleBinding | EMR rolebinding in `spark` namespace |

### Status

- [ ] EMR Virtual Cluster verified
- [ ] Execution IAM role verified
- [ ] CloudWatch log group verified
- [ ] Kubernetes RBAC verified

---

## Task 3 — ECR Repositories

### Verify

```bash
# List all ECR repositories
aws ecr describe-repositories \
  --query "repositories[?starts_with(repositoryName,'lakehouse-at-scale/')].{Name:repositoryName,URI:repositoryUri,ScanOnPush:imageScanningConfiguration.scanOnPush}" \
  --profile non-prod --region ap-southeast-1
```

| Check | Expected |
|---|---|
| `lakehouse-at-scale/de-team-base` | exists, scan on push enabled |
| `lakehouse-at-scale/de-team-code` | exists, scan on push enabled |
| `lakehouse-at-scale/sales-team-base` | exists, scan on push enabled |
| `lakehouse-at-scale/sales-team-code` | exists, scan on push enabled |
| Lifecycle policy | keep last 30 images |
| Tag mutability | `MUTABLE` |

### Status

- [ ] 4 ECR repositories verified
- [ ] Lifecycle policies verified
- [ ] Image scanning enabled

---

## Task 3 — S3 Buckets

### Verify

```bash
# Data Lake bucket
aws s3api head-bucket --bucket lakehouse-at-scale-data-lake --profile non-prod 2>&1 && echo "EXISTS" || echo "NOT FOUND"

# Pipes bucket
aws s3api head-bucket --bucket lakehouse-at-scale-pipes --profile non-prod 2>&1 && echo "EXISTS" || echo "NOT FOUND"

# Spark logs bucket
aws s3api head-bucket --bucket lakehouse-at-scale-spark-logs --profile non-prod 2>&1 && echo "EXISTS" || echo "NOT FOUND"

# Check versioning (example for data lake)
aws s3api get-bucket-versioning --bucket lakehouse-at-scale-data-lake --profile non-prod

# Check encryption
aws s3api get-bucket-encryption --bucket lakehouse-at-scale-data-lake --profile non-prod

# Check public access block
aws s3api get-public-access-block --bucket lakehouse-at-scale-data-lake --profile non-prod
```

| Check | Expected |
|---|---|
| `lakehouse-at-scale-data-lake` | exists |
| `lakehouse-at-scale-pipes` | exists |
| `lakehouse-at-scale-spark-logs` | exists |
| Versioning | `Enabled` on all buckets |
| Encryption | `AES256` (SSE-S3) on all buckets |
| Public access | all blocked on all buckets |

Each bucket is a separate Terragrunt unit (`s3-data-lake/`, `s3-pipes/`, `s3-spark-logs/`) using community module directly.

### Status

- [ ] Data lake bucket verified
- [ ] Pipes bucket verified
- [ ] Spark logs bucket verified
- [ ] Versioning enabled
- [ ] Encryption enabled
- [ ] Public access blocked

---

## Task 3 — IAM Policies + IRSA Roles (Dagster)

### Verify

```bash
# Dagster de-team policy
aws iam list-policies \
  --query "Policies[?PolicyName=='lakehouse-at-scale-dagster-de-team'].{Name:PolicyName,Arn:Arn}" \
  --scope Local --profile non-prod

# Dagster sales-team policy
aws iam list-policies \
  --query "Policies[?PolicyName=='lakehouse-at-scale-dagster-sales-team'].{Name:PolicyName,Arn:Arn}" \
  --scope Local --profile non-prod

# Dagster de-team IRSA role
aws iam get-role \
  --role-name lakehouse-at-scale-dagster-de-team \
  --query "Role.{RoleName:RoleName,Arn:Arn}" \
  --profile non-prod

# Dagster sales-team IRSA role
aws iam get-role \
  --role-name lakehouse-at-scale-dagster-sales-team \
  --query "Role.{RoleName:RoleName,Arn:Arn}" \
  --profile non-prod

# Check de-team trust policy (IRSA)
aws iam get-role \
  --role-name lakehouse-at-scale-dagster-de-team \
  --query "Role.AssumeRolePolicyDocument" \
  --profile non-prod
```

| Check | Expected |
|---|---|
| de-team policy | exists, EMR + S3 Pipes + S3 logs permissions |
| sales-team policy | exists, Athena + S3 data lake + Glue permissions |
| de-team IRSA role | exists, OIDC trust for `dagster:dagster-de-team` |
| sales-team IRSA role | exists, OIDC trust for `dagster:dagster-sales-team` |

Each policy and role is a separate Terragrunt unit using community `terraform-aws-modules/iam/aws` submodules.

### Status

- [ ] de-team IAM policy verified
- [ ] sales-team IAM policy verified
- [ ] de-team IRSA role verified
- [ ] sales-team IRSA role verified
- [ ] Trust policies verified (OIDC)

---

## Task 3 — Glue Data Catalog

### Verify

```bash
# List Glue databases
aws glue get-databases \
  --query "DatabaseList[?starts_with(Name,'lakehouse_at_scale_')].{Name:Name,LocationUri:LocationUri}" \
  --profile non-prod --region ap-southeast-1
```

| Check | Expected |
|---|---|
| `lakehouse_at_scale_staging` | exists, location `s3://lakehouse-at-scale-data-lake/warehouse/staging/` |
| `lakehouse_at_scale_intermediate` | exists, location `s3://lakehouse-at-scale-data-lake/warehouse/intermediate/` |
| `lakehouse_at_scale_marts` | exists, location `s3://lakehouse-at-scale-data-lake/warehouse/marts/` |

### Status

- [ ] staging database verified
- [ ] intermediate database verified
- [ ] marts database verified

---

## Notes

- Karpenter controller is **NOT running yet** — Helm chart + NodePool CRDs + EC2NodeClass are defined in `argocd/karpenter/` but ArgoCD has not been bootstrapped yet
- `spark` and `dagster` namespaces are defined in `argocd/namespaces/` but not yet created on the cluster
- EMR execution role is created by the EMR community module, NOT the IAM module
- Dagster IRSA roles require the `spark` and `dagster` namespaces + service accounts to exist (created by ArgoCD)
- ArgoCD App-of-Apps charts are ready — bootstrap with `kubectl apply -f argocd/app-of-apps.yaml` after ArgoCD is installed on the cluster
- Placeholder values in ArgoCD charts (cluster name, IRSA role ARNs, ECR URLs) need to be populated from Terraform outputs before deploying
- Current state: all AWS resources for tasks 1–3 ready, Docker images for task 5 ready, ArgoCD charts for task 6 ready, dbt projects for task 8 ready, de-team Dagster application code for task 9 ready, sales-team Dagster application code for task 10 ready, application code import validation (task 11) passed (dagster-aws 0.29.3, dagster 1.13.3, dagster-dbt 0.29.3), CI/CD pipeline for task 12 ready
- To destroy all resources: `cd infra/non-prod/ap-southeast-1 && terragrunt run --all destroy`

---

## Task 5 — Docker Images

### Verify

```bash
# Build de-team base image (from Application Code Repo)
cd dbt-dagster-project/de-team
docker build -f Dockerfile.base -t de-team-base:test .

# Build de-team code image
docker build -f Dockerfile.code --build-arg BASE_IMAGE=de-team-base:test -t de-team-code:test .

# Build sales-team base image
cd dbt-dagster-project/sales-team
docker build -f Dockerfile.base -t sales-team-base:test .

# Build sales-team code image
docker build -f Dockerfile.code --build-arg BASE_IMAGE=sales-team-base:test -t sales-team-code:test .

# Validate docker-image Terraform module
cd infra/modules/docker-image
terraform init
terraform validate
```

| Check | Expected |
|---|---|
| de-team Dockerfile.base | Builds successfully from `public.ecr.aws/emr-on-eks/spark/emr-7.13.0` |
| de-team Dockerfile.code | Builds in <30s from base image, COPY only |
| sales-team Dockerfile.base | Builds successfully from `python:3.10-slim` |
| sales-team Dockerfile.code | Builds in <30s from base image, COPY only |
| docker-image module | `terraform validate` passes |
| de-team base packages | dbt-core, dbt-spark, dagster-pipes, boto3, Iceberg JAR |
| sales-team base packages | dbt-core, dbt-athena-community, dagster, dagster-aws, dagster-dbt, boto3 |

### Status

- [ ] de-team Dockerfile.base builds
- [ ] de-team Dockerfile.code builds (<30s)
- [ ] sales-team Dockerfile.base builds
- [ ] sales-team Dockerfile.code builds (<30s)
- [ ] docker-image Terraform module validates

---

## Task 6 — ArgoCD App-of-Apps

### Verify

```bash
# Validate App-of-Apps root chart (no dependencies needed)
helm template argocd/apps/

# Validate Karpenter chart
helm dependency build argocd/karpenter/
helm template argocd/karpenter/

# Validate Dagster chart
helm dependency build argocd/dagster/
helm template argocd/dagster/

# Check namespace manifests are valid YAML
kubectl apply --dry-run=client -f argocd/namespaces/dagster-ns.yaml
kubectl apply --dry-run=client -f argocd/namespaces/spark-ns.yaml

# Check bootstrap Application CRD
kubectl apply --dry-run=client -f argocd/app-of-apps.yaml
```

| Check | Expected |
|---|---|
| `helm template argocd/apps/` | Renders 1 AppProject + 3 Applications (namespaces, karpenter, dagster) |
| `helm template argocd/karpenter/` | Renders Karpenter chart + 2 NodePools + 1 EC2NodeClass |
| `helm template argocd/dagster/` | Renders Dagster chart with 2 user code deployments |
| Sync waves | namespaces (1) → karpenter (2) → dagster (3) |
| AppProject namespaces | `dagster`, `spark`, `kube-system`, `argocd` |
| Karpenter spark-executors | Spot, m5.xlarge/2xlarge + m6i.xlarge/2xlarge, taint, 300s consolidate |
| Karpenter spark-drivers | On-Demand, m5.large + m6i.large, 120s consolidate |
| Dagster de-team | Service account `dagster-de-team` with IRSA annotation |
| Dagster sales-team | Service account `dagster-sales-team` with IRSA annotation |
| Bootstrap CRD | Points to `argocd/apps` path, auto-sync enabled |

### Status

- [ ] App-of-Apps root chart renders correctly
- [ ] Karpenter chart renders correctly
- [ ] Dagster chart renders correctly
- [ ] Namespace manifests valid
- [ ] Bootstrap Application CRD valid

---

## Task 8 — dbt Projects

### Verify

```bash
# Validate de-team dbt project YAML
cd dbt-dagster-project/de-team/dbt_project
python -c "import yaml; yaml.safe_load(open('dbt_project.yml'))" && echo "dbt_project.yml OK"
python -c "import yaml; yaml.safe_load(open('profiles.yml'))" && echo "profiles.yml OK"

# Validate sales-team dbt project YAML
cd dbt-dagster-project/sales-team/dbt_project
python -c "import yaml; yaml.safe_load(open('dbt_project.yml'))" && echo "dbt_project.yml OK"
python -c "import yaml; yaml.safe_load(open('profiles.yml'))" && echo "profiles.yml OK"

# Check de-team model files exist
ls dbt-dagster-project/de-team/dbt_project/models/staging/stg_raw_orders.sql
ls dbt-dagster-project/de-team/dbt_project/models/marts/orders.sql
ls dbt-dagster-project/de-team/dbt_project/models/marts/orders.yml

# Check sales-team model files exist
ls dbt-dagster-project/sales-team/dbt_project/models/staging/stg_sales.sql
ls dbt-dagster-project/sales-team/dbt_project/models/staging/schema.yml

# Check generate_schema_name macro exists for both teams
ls dbt-dagster-project/de-team/dbt_project/macros/generate_schema_name.sql
ls dbt-dagster-project/sales-team/dbt_project/macros/generate_schema_name.sql
```

| Check | Expected |
|---|---|
| de-team `dbt_project.yml` | Valid YAML, project `de_team_lakehouse`, default `table` + `iceberg` |
| de-team `profiles.yml` | dbt-spark, `method: session` |
| de-team `stg_raw_orders.sql` | References `{{ source('raw', 'raw_orders') }}` |
| de-team `orders.sql` | Incremental merge, `unique_key: order_id`, references `{{ ref('stg_raw_orders') }}` |
| de-team `orders.yml` | `meta.spark_config` with driver_cpu: 2, executor_instances: 4 |
| de-team `generate_schema_name.sql` | Maps custom schemas to Glue DB names |
| sales-team `dbt_project.yml` | Valid YAML, project `sales_team_lakehouse`, default `table` + `iceberg` |
| sales-team `profiles.yml` | dbt-athena, `type: athena`, s3_staging_dir |
| sales-team `stg_sales.sql` | References `{{ source('raw', 'raw_sales') }}` |
| sales-team `generate_schema_name.sql` | Maps custom schemas to Glue DB names |

### Status

- [x] de-team dbt project created (dbt-spark + Iceberg)
- [x] de-team sample models created (staging + marts with dependency)
- [x] de-team incremental model with merge strategy
- [x] de-team spark_config meta on orders model
- [x] sales-team dbt project created (dbt-athena + Iceberg)
- [x] sales-team sample model created (staging)

---

## Task 9 — Dagster Application Code (de-team)

### Verify

```bash
# Check all files exist
ls dbt-dagster-project/de-team/dagster_project/__init__.py
ls dbt-dagster-project/de-team/dagster_project/definitions.py
ls dbt-dagster-project/de-team/dagster_project/assets/__init__.py
ls dbt-dagster-project/de-team/dagster_project/assets/dbt_assets.py
ls dbt-dagster-project/de-team/dagster_project/assets/python_assets.py
ls dbt-dagster-project/de-team/dagster_project/resources/__init__.py
ls dbt-dagster-project/de-team/dagster_project/utils/__init__.py
ls dbt-dagster-project/de-team/dagster_project/utils/spark_config.py
ls dbt-dagster-project/de-team/spark_entrypoint/entrypoint.py

# Verify Python imports (requires dagster, dagster-dbt, dagster-aws installed)
cd dbt-dagster-project/de-team
python -c "from dagster_project.utils.spark_config import SparkConfigManager, SparkJobConfig, SparkResourceConfig; print('spark_config OK')"
python -c "from dagster_project.resources import create_pipes_emr_client; print('resources OK')"
```

| Check | Expected |
|---|---|
| `spark_config.py` | `SparkResourceConfig`, `SparkJobConfig`, `SparkConfigManager`, `DEFAULT_SPARK_PROPERTIES` |
| `SparkConfigManager.merge_config()` | Partial override with default fallback, None → full default |
| `SparkConfigManager.build_start_job_run_params()` | Returns dict with releaseLabel, virtualClusterId, executionRoleArn, jobDriver |
| `dbt_assets.py` | `@dbt_assets` with `SparkDbtTranslator`, submits via `PipesEMRContainersClient` |
| `python_assets.py` | `orders_validation` asset with `deps=["orders"]` |
| `entrypoint.py` | `open_dagster_pipes` + `dbtRunner` + `report_asset_materialization` |
| `resources/__init__.py` | `create_pipes_emr_client()` with `PipesS3MessageReader` |
| `definitions.py` | `dg.Definitions` with all assets + resources |

### Status

- [x] SparkConfigManager implemented (merge + build_start_job_run_params)
- [x] dbt_assets.py implemented (@dbt_assets + PipesEMRContainersClient)
- [x] Spark entrypoint implemented (dbt build via Pipes)
- [x] Python-only assets implemented (orders_validation)
- [x] PipesEMRContainersClient resource implemented
- [x] Dagster Definitions entry point implemented
- [x] All __init__.py files created

## Task 10 — Dagster Application Code (sales-team)

### Verify

```bash
# Check all files exist
ls dbt-dagster-project/sales-team/dagster_project/__init__.py
ls dbt-dagster-project/sales-team/dagster_project/definitions.py
ls dbt-dagster-project/sales-team/dagster_project/assets/__init__.py
ls dbt-dagster-project/sales-team/dagster_project/assets/dbt_assets.py
ls dbt-dagster-project/sales-team/dagster_project/assets/python_assets.py
ls dbt-dagster-project/sales-team/dagster_project/resources/__init__.py

# Verify Python imports (requires dagster, dagster-dbt installed)
cd dbt-dagster-project/sales-team
python -c "from dagster_project.resources import create_dbt_cli_resource; print('resources OK')"
```

| Check | Expected |
|---|---|
| `dbt_assets.py` | `@dbt_assets` with `DbtCliResource`, runs `dbt build` via `dbt_cli.cli()` |
| `python_assets.py` | `sales_data_validation` asset with `deps=["stg_sales"]` |
| `resources/__init__.py` | `create_dbt_cli_resource()` returning `DbtCliResource` |
| `definitions.py` | `dg.Definitions` with all assets + `dbt_cli` resource |
| No Spark/EMR deps | sales-team uses DbtCliResource only (Athena), no PipesEMRContainersClient |

### Status

- [x] dbt_assets.py implemented (@dbt_assets + DbtCliResource)
- [x] Python-only assets implemented (sales_data_validation)
- [x] DbtCliResource resource implemented
- [x] Dagster Definitions entry point implemented
- [x] All __init__.py files created

---

## Task 11 — Checkpoint: Application Code Validation

### Verify

```bash
# de-team: verify Dagster definitions import
$env:PYTHONPATH = "dbt-dagster-project/de-team"; python -c "from dagster_project.definitions import defs; print('de-team import OK')"

# sales-team: verify Dagster definitions import
$env:PYTHONPATH = "dbt-dagster-project/sales-team"; python -c "from dagster_project.definitions import defs; print('sales-team import OK')"
```

| Check | Expected |
|---|---|
| de-team `from dagster_project.definitions import defs` | Imports successfully |
| de-team resources | `pipes_emr_containers_client`, `spark_config_manager` |
| sales-team `from dagster_project.definitions import defs` | Imports successfully |
| sales-team resources | `dbt_cli` |

### Fixes Applied During Validation

| Issue | Root Cause | Fix |
|---|---|---|
| `DagsterInvalidDefinitionError: Invalid asset dependencies: spark_config_manager` | `SparkConfigManager` was a plain Python class, not a Dagster resource | Converted to `dg.ConfigurableResource` subclass with flat Pydantic fields |
| `Cannot annotate context parameter with type dg.AssetExecutionContext` | `from __future__ import annotations` makes type hints lazy strings; Dagster's decorator introspection needs actual type objects | Removed `from __future__ import annotations` from all Dagster asset/definition files |
| `Cannot set database in spark!` | dbt-spark source had `database: glue_catalog` which is not supported | Removed `database` field from staging source definition |

### Status

- [x] de-team definitions import verified
- [x] sales-team definitions import verified
- [x] All resources registered correctly

---

## Task 12 — CI/CD Pipeline (GitHub Actions)

### Verify

```bash
# Validate workflow YAML syntax
python -c "import yaml; yaml.safe_load(open('.github/workflows/ci-cd.yml'))" && echo "ci-cd.yml OK"

# Check workflow file exists
ls .github/workflows/ci-cd.yml
```

| Check | Expected |
|---|---|
| Workflow trigger | `push` to `main`, path filters for `de-team/**` and `sales-team/**` |
| `detect-changes` job | Uses `dorny/paths-filter@v3` to detect which code locations changed |
| `build-de-team` job | Conditional on de-team changes, runs `dagster-dbt prepare-and-package`, builds Code Image, pushes to ECR with git SHA tag |
| `build-sales-team` job | Conditional on sales-team changes, same flow for sales-team |
| `update-argocd` job | Runs if any build succeeded, updates image tag in ArgoCD Dagster Helm values, pushes commit |
| AWS auth | OIDC (`aws-actions/configure-aws-credentials@v4`) |
| ECR login | `aws-actions/amazon-ecr-login@v2` |
| Image tag format | First 8 chars of git commit SHA |
| Path isolation | Only rebuilds changed code location(s) |
| ArgoCD update | Uses `yq` to surgically update only the changed deployment's image tag |

### Required GitHub Secrets

| Secret | How to obtain |
|---|---|
| `AWS_ACCOUNT_ID` | AWS account ID (`560503716668` for non-prod). Run `aws sts get-caller-identity --query Account --output text --profile non-prod` |
| `AWS_OIDC_ROLE_ARN` | Managed by Terragrunt: `cd infra/non-prod/ap-southeast-1/github-oidc && terragrunt run --all apply`, then `cd role && terragrunt output arn` |
| `ARGOCD_REPO` | `owner/repo` path of the ArgoCD App Repo on GitHub (e.g. `thanhtt-demo/argocd-app-repo`) |
| `ARGOCD_REPO_PAT` | GitHub Personal Access Token with `contents: write` on the ArgoCD App Repo. Create at GitHub → Settings → Developer settings → Personal access tokens |

Add secrets at: GitHub repo → Settings → Secrets and variables → Actions → New repository secret.

### Status

- [x] GitHub Actions workflow created
- [ ] Workflow YAML syntax validated
- [ ] GitHub secrets configured
- [ ] End-to-end test (push code → image built → ArgoCD updated)
