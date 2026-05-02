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

- Karpenter controller is **NOT running yet** — Helm chart + NodePool CRDs + EC2NodeClass will be deployed by ArgoCD (task 6)
- `spark` namespace does **NOT exist yet** — will be created by ArgoCD (task 6). EMR Virtual Cluster is configured with `create_namespace = false`
- EMR execution role is created by the EMR community module, NOT the IAM module
- Dagster IRSA roles require the `spark` and `dagster` namespaces + service accounts to exist (created by ArgoCD)
- Current state: all AWS resources for tasks 1–3 ready, waiting for ArgoCD to deploy Kubernetes workloads
- To destroy all resources: `cd infra/non-prod/ap-southeast-1 && terragrunt run --all destroy`
