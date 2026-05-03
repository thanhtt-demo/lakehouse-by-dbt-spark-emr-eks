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
- Current state: all AWS resources for tasks 1–3 ready, Docker images for task 5 ready, ArgoCD charts for task 6 ready
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
