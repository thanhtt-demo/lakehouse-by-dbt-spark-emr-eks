# Implementation Plan: dbt-dagster-lakehouse

## Tổng quan

Kế hoạch triển khai hệ thống dbt-dagster-lakehouse theo thứ tự ưu tiên: Infrastructure (Terraform/Terragrunt) → Docker Images → ArgoCD → Application Code → CI/CD → Testing. Mỗi task xây dựng trên các task trước đó, đảm bảo không có code orphan.

## Tasks

- [x] 1. Thiết lập cấu trúc Terragrunt root và VPC module
  - [x] 1.1 Tạo cấu trúc thư mục Terragrunt root
    - Tạo file `infra/terragrunt.hcl` root config với remote state (S3 bucket + DynamoDB table cho state locking), provider generation block
    - Tạo file `infra/non-prod/{account}/{region}/env.hcl` với biến `aws_account_id`, `aws_profile`
    - Tạo thư mục `infra/_envcommon/` và `infra/modules/`
    - _Requirements: 6.1, 6.2, 6.3_

  - [x] 1.2 Tạo Terraform module VPC (`infra/modules/vpc/`)
    - Tạo `main.tf`: VPC, public/private subnets (multi-AZ), NAT Gateway, Internet Gateway, route tables
    - Tạo `variables.tf`: vpc_cidr, availability_zones, private_subnet_cidrs, public_subnet_cidrs
    - Tạo `outputs.tf`: vpc_id, private_subnet_ids, public_subnet_ids
    - _Requirements: 6.4_

  - [x] 1.3 Tạo Terragrunt config cho VPC
    - Tạo `infra/_envcommon/vpc.hcl` shared config
    - Tạo `infra/non-prod/{account}/{region}/vpc/terragrunt.hcl` với include root config và _envcommon
    - _Requirements: 6.1, 6.5_

- [x] 2. Tạo EKS Cluster và Karpenter AWS resources
  - [x] 2.1 Tạo EKS cluster (community module `terraform-aws-modules/eks/aws` v21.19.0)
    - Gọi thẳng community module từ `_envcommon/eks.hcl` via `tfr:///` — không cần local wrapper module
    - EKS cluster v1.35, managed node group (system workloads), OIDC provider (IRSA), addons (vpc-cni, coredns, kube-proxy, eks-pod-identity-agent)
    - Node security group tagged `karpenter.sh/discovery` cho Karpenter auto-discovery
    - Dependency: VPC module (vpc_id, private_subnet_ids) với mock_outputs cho plan
    - _Requirements: 5.1, 6.4, 6.5_

  - [x] 2.2 Tạo Terragrunt config cho EKS
    - Tạo `infra/_envcommon/eks.hcl` — source `tfr:///terraform-aws-modules/eks/aws?version=21.19.0`
    - Tạo `infra/non-prod/ap-southeast-1/eks/terragrunt.hcl` với dependency VPC + mock_outputs
    - _Requirements: 6.1, 6.5_

  - [x] 2.3 Tạo Karpenter AWS resources (community submodule `terraform-aws-modules/eks/aws//modules/karpenter` v21.19.0)
    - Gọi thẳng community submodule từ `_envcommon/karpenter.hcl` via `tfr:///` — không cần local wrapper module
    - Chỉ tạo AWS resources: IAM roles (controller + node), SQS queue, EventBridge rules, access entry
    - Karpenter Helm chart, NodePool CRDs, EC2NodeClass sẽ được deploy bởi ArgoCD (task 6)
    - Dependency: EKS module (cluster_name) với mock_outputs cho plan
    - _Requirements: 5.1, 9.4_

  - [x] 2.4 Tạo Terragrunt config cho Karpenter
    - Tạo `infra/_envcommon/karpenter.hcl` — source `tfr:///terraform-aws-modules/eks/aws//modules/karpenter?version=21.19.0`
    - Tạo `infra/non-prod/ap-southeast-1/karpenter/terragrunt.hcl` với dependency EKS + mock_outputs
    - _Requirements: 6.1, 6.5_

- [x] 3. Tạo EMR Virtual Cluster, ECR, S3, IAM, và Glue modules
  - [x] 3.1 Tạo EMR Virtual Cluster config (community module `terraform-aws-modules/emr/aws//modules/virtual-cluster` v3.3.0)
    - Gọi thẳng community submodule từ `_envcommon/emr-virtual-cluster.hcl` via `tfr:///` — không cần local wrapper module
    - Community module đã bao gồm: EMR Virtual Cluster, Kubernetes RBAC (Role, RoleBinding), IAM execution role (đọc/ghi S3, Glue Catalog), CloudWatch log group
    - Cấu hình: namespace `spark`, `create_namespace = false` (namespace do ArgoCD quản lý), `create_iam_role = true` với `s3_bucket_arns` cho data lake + pipes buckets
    - Dependency: EKS module (cluster_name, oidc_provider_arn)
    - _Requirements: 7.1, 7.2, 7.4_

  - [x] 3.2 Tạo ECR config (community module `terraform-aws-modules/ecr/aws` v3.2.0)
    - Gọi thẳng community module từ `_envcommon/ecr.hcl` via `tfr:///` — không cần local wrapper module
    - 4 ECR repositories (de-team-base, de-team-code, sales-team-base, sales-team-code), lifecycle policy (giữ N images gần nhất), image scanning on push
    - _Requirements: 8.1, 8.3, 7.3_

  - [x] 3.3 Tạo S3 config (community module `terraform-aws-modules/s3-bucket/aws` v5.12.0)
    - Gọi thẳng community module từ 3 envcommon files riêng biệt via `tfr:///` — không cần local wrapper module
    - 3 Terragrunt units riêng: `s3-data-lake/`, `s3-pipes/`, `s3-spark-logs/`
    - Mỗi bucket: versioning enabled, encryption SSE-S3, block public access
    - _Requirements: 4.2, 10.2, 11.4_

  - [x] 3.4 Tạo IAM Policies + IRSA Roles (community modules `terraform-aws-modules/iam/aws` v6.6.0)
    - Dùng community submodules `iam-policy` và `iam-role-for-service-accounts` via `tfr:///`
    - 4 Terragrunt units riêng: `dagster-de-team-policy/`, `dagster-de-team-irsa/`, `dagster-sales-team-policy/`, `dagster-sales-team-irsa/`
    - EMR execution role đã được tạo bởi community EMR virtual-cluster submodule (task 3.1)
    - Karpenter IAM roles đã được tạo bởi community submodule (task 2.3)
    - Dependencies: Policy → S3 buckets + EMR; IRSA → EKS + Policy
    - _Requirements: 9.3, 7.2_

  - [x] 3.5 Tạo Terraform module Glue (`infra/modules/glue/`)
    - Local module vì không có community module cho Glue Data Catalog databases, và logic đơn giản
    - Tạo `main.tf`: Glue Data Catalog databases cho dbt schemas (staging, intermediate, marts)
    - Tạo `variables.tf`: database_names, data_lake_bucket
    - Tạo `outputs.tf`: database_names, catalog_id
    - Dependency: S3 module
    - _Requirements: 4.3, 12.4_

  - [x] 3.6 Tạo Terragrunt configs cho EMR, ECR, S3, IAM, Glue
    - Tạo `infra/_envcommon/` files cho tất cả services — tất cả dùng community modules via `tfr:///`
    - S3: 3 envcommon files (`s3-data-lake.hcl`, `s3-pipes.hcl`, `s3-spark-logs.hcl`) → 3 live configs
    - IAM: 4 envcommon files (`dagster-de-team-policy.hcl`, `dagster-de-team-irsa.hcl`, `dagster-sales-team-policy.hcl`, `dagster-sales-team-irsa.hcl`) → 4 live configs
    - EMR, ECR, Glue: 1 envcommon + 1 live config mỗi service
    - Dependencies:
      - EMR → EKS, S3 buckets
      - Policy → S3 buckets, EMR
      - IRSA → EKS, Policy
      - Glue → S3 data lake
      - ECR, S3 buckets → (không dependency)
    - _Requirements: 6.1, 6.4, 6.5_

- [ ] 4. Checkpoint — Xác nhận toàn bộ Terraform modules
  - Chạy `terraform validate` cho mỗi module trong `infra/modules/`
  - Chạy `terragrunt graph-dependencies` để xác nhận dependency graph đúng
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Tạo Docker Images — Base Images và Code Images
  - [x] 5.1 Tạo Dockerfile.base cho de-team
    - Tạo `dbt-dagster-project/de-team/Dockerfile.base`:
      - FROM EMR Spark base image (public.ecr.aws/emr-on-eks/spark/emr-7.13.0)
      - Cài Python 3.10, dbt-core, dbt-spark, dagster-pipes, boto3
      - COPY Iceberg Spark runtime JAR vào `/opt/spark/jars/`
      - Không chứa code project
    - _Requirements: 8.1, 8.2, 10.6, 18.4_

  - [x] 5.2 Tạo Dockerfile.base cho sales-team
    - Tạo `dbt-dagster-project/sales-team/Dockerfile.base`:
      - FROM python:3.10-slim
      - Cài dbt-core, dbt-athena-community, dagster, dagster-aws, boto3
      - Dependencies KHÁC de-team (không cần Spark)
    - _Requirements: 8.1, 18.5_

  - [x] 5.3 Tạo Dockerfile.code cho de-team
    - Tạo `dbt-dagster-project/de-team/Dockerfile.code`:
      - FROM de-team-base:latest
      - COPY dbt_project/ /app/dbt_project/
      - COPY spark_entrypoint/entrypoint.py /app/entrypoint.py
      - COPY dagster_project/ /app/dagster_project/
      - Không cài thêm packages — build <30s
    - _Requirements: 8.3, 8.4, 8.5_

  - [x] 5.4 Tạo Dockerfile.code cho sales-team
    - Tạo `dbt-dagster-project/sales-team/Dockerfile.code`:
      - FROM sales-team-base:latest
      - COPY dbt_project/ /app/dbt_project/
      - COPY dagster_project/ /app/dagster_project/
      - Không cài thêm packages — build <30s
    - _Requirements: 8.3, 8.4, 8.5_

  - [x] 5.5 Tạo Terraform module docker-image (`infra/modules/docker-image/`)
    - Tạo module sử dụng `null_resource` + `local-exec` provisioner để build và push Base_Image lên ECR
    - Chỉ rebuild khi thay đổi dependency versions (trigger bằng hash của Dockerfile.base)
    - Tạo `variables.tf`: dockerfile_path, ecr_repository_url, image_tag
    - Dependency: ECR module
    - _Requirements: 8.2_

- [x] 6. Thiết lập ArgoCD App-of-Apps
  - [x] 6.1 Tạo ArgoCD App-of-Apps root Helm chart
    - Tạo `argocd/apps/Chart.yaml` với metadata
    - Tạo `argocd/apps/values.yaml` — single source of truth cho tất cả applications: namespaces (syncWave 1), karpenter (syncWave 2), dagster (syncWave 3)
    - Tạo `argocd/apps/templates/applications.yaml` — loop over values → tạo ArgoCD Application CRDs
    - Tạo `argocd/apps/templates/project.yaml` — ArgoCD AppProject với quyền truy cập namespaces `dagster`, `spark`, `kube-system`
    - _Requirements: 14.2, 16.1, 16.2, 16.3, 16.4_

  - [x] 6.2 Tạo Karpenter Helm chart và CRDs (managed by ArgoCD)
    - Tạo `argocd/karpenter/Chart.yaml` với dependency: official Karpenter Helm chart (`oci://public.ecr.aws/karpenter/karpenter`)
    - Tạo `argocd/karpenter/values.yaml` với cấu hình:
      - nodeSelector: `karpenter.sh/controller: "true"` (chạy trên system nodes)
      - settings: clusterName, clusterEndpoint, interruptionQueue (từ Terraform outputs)
    - Tạo `argocd/karpenter/templates/ec2nodeclass.yaml` — EC2NodeClass (AL2023, subnet/SG discovery via tags)
    - Tạo `argocd/karpenter/templates/nodepool-spark-executors.yaml` — NodePool Spot (m5.xlarge/2xlarge, m6i.xlarge/2xlarge), taint `spark-role=executor:NoSchedule`, consolidateAfter 300s
    - Tạo `argocd/karpenter/templates/nodepool-spark-drivers.yaml` — NodePool On-Demand (m5.large, m6i.large), consolidateAfter 120s
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [x] 6.3 Tạo Dagster umbrella Helm chart
    - Tạo `argocd/dagster/Chart.yaml` với dependency: official dagster Helm chart
    - Tạo `argocd/dagster/values.yaml` với cấu hình:
      - dagsterWebserver (replicaCount: 1)
      - dagsterDaemon (replicaCount: 1)
      - dagsterUserDeployments: 2 entries (de-team, sales-team) — mỗi entry có image repo/tag, dagsterApiGrpcArgs, port, resources, serviceAccount với IRSA annotation
    - _Requirements: 15.1, 15.2, 15.3, 15.5, 18.8_

  - [x] 6.4 Tạo Namespace manifests và bootstrap file
    - Tạo `argocd/namespaces/dagster-ns.yaml` và `argocd/namespaces/spark-ns.yaml`
    - Tạo `argocd/app-of-apps.yaml` — bootstrap Application CRD (apply once để deploy everything)
    - _Requirements: 14.3, 14.4_

- [ ] 7. Checkpoint — Xác nhận ArgoCD Helm charts
  - Chạy `helm template argocd/apps/` để validate App-of-Apps chart render đúng
  - Chạy `helm template argocd/karpenter/` để validate Karpenter chart render đúng (Helm + 2 NodePools + EC2NodeClass)
  - Chạy `helm template argocd/dagster/` để validate Dagster chart render đúng (2 code locations)
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Triển khai dbt Projects
  - [x] 8.1 Tạo dbt project cho de-team (dbt-spark)
    - Tạo `dbt-dagster-project/de-team/dbt_project/dbt_project.yml`: project name, version, materialization mặc định `table` với `file_format: iceberg`
    - Tạo `dbt-dagster-project/de-team/dbt_project/profiles.yml`: dbt-spark adapter với `method: session` (kết nối SparkSession active trong cùng process)
    - Tạo cấu trúc thư mục models: `staging/`, `intermediate/`, `marts/`
    - Tạo schema mapping cho Glue Data Catalog databases
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 4.1_

  - [x] 8.2 Tạo sample dbt models cho de-team
    - Tạo ít nhất 2 sample models (staging + marts) với dependency giữa chúng
    - Tạo model YAML với `meta.spark_config` cho model cần custom Spark resources (ví dụ: `orders.yml` với driver_cpu, executor_instances)
    - Tạo model sử dụng incremental materialization với `incremental_strategy: merge`, `unique_key`
    - _Requirements: 2.1, 2.2, 4.4_

  - [x] 8.3 Tạo dbt project cho sales-team (dbt-athena)
    - Tạo `dbt-dagster-project/sales-team/dbt_project/dbt_project.yml`: project name, materialization mặc định `table` với `file_format: iceberg`
    - Tạo `dbt-dagster-project/sales-team/dbt_project/profiles.yml`: dbt-athena adapter
    - Tạo cấu trúc thư mục models và ít nhất 1 sample model
    - _Requirements: 18.3_

- [x] 9. Triển khai Dagster Application Code — de-team
  - [x] 9.1 Tạo SparkConfigManager (`de-team/dagster_project/utils/spark_config.py`)
    - Implement class `SparkResourceConfig` (dataclass) với default values: driver_cpu="1", driver_memory="2g", executor_cpu="1", executor_memory="4g", executor_instances=2
    - Implement class `SparkJobConfig` (dataclass) với `resources: SparkResourceConfig` và `spark_properties: dict[str, str]`
    - Implement class `SparkConfigManager` với:
      - `__init__(self, default_config: SparkJobConfig)`
      - `merge_config(self, model_meta: dict | None) -> SparkJobConfig` — merge model meta với default, model values ưu tiên
      - `build_start_job_run_params(...)` — tạo dict cho PipesEMRContainersClient.run() với releaseLabel, virtualClusterId, executionRoleArn, jobDriver.sparkSubmitJobDriver
    - Implement DEFAULT_SPARK_PROPERTIES dict với Iceberg + Glue catalog configs
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 11.1, 11.2, 11.3_

  - [x] 9.2 Tạo dbt_assets.py (`de-team/dagster_project/assets/dbt_assets.py`)
    - Implement `DbtProject` initialization với `project_dir` và `packaged_project_dir` (prod: precompiled manifest)
    - Implement `SparkDbtTranslator(DagsterDbtTranslator)` — custom translator inject `spark_config` từ dbt model meta vào Dagster asset metadata
    - Implement `@dbt_assets` decorated function `de_team_dbt_assets`:
      - Iterate `context.selected_asset_keys`
      - Tìm model trong manifest, đọc `spark_config` từ meta
      - Gọi `spark_config_manager.merge_config()` và `build_start_job_run_params()`
      - Submit Spark job qua `pipes_emr_containers_client.run()`
      - Yield events từ `invocation.get_results()`
    - Implement helper functions: `get_parsed_manifest()`, `_find_model_in_manifest()`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 3.1, 10.3_

  - [x] 9.3 Tạo Spark entrypoint script (`de-team/spark_entrypoint/entrypoint.py`)
    - Implement `main()`:
      - Khởi tạo `open_dagster_pipes` với `PipesS3MessageWriter`
      - Lấy `model_name` và `dbt_command` từ Pipes extras
      - Tạo SparkSession (đã có sẵn từ spark-submit)
      - Chạy `dbt build --select model_name` qua `dbtRunner` (Python API, cùng process với SparkSession)
      - Parse `run_results.json` và report test results as `AssetCheckResult` qua Pipes
      - Report `asset_materialization` với metadata (model_name, emr_console_url, test_count)
      - Cleanup: `spark.stop()`
    - Implement `_report_test_results()` và `_parse_run_results()` helper functions
    - _Requirements: 3.3, 3.4, 3.5, 10.4, 10.5, 12.1_

  - [x] 9.4 Tạo Python-only assets (`de-team/dagster_project/assets/python_assets.py`)
    - Tạo ít nhất 1 sample Python-only asset chạy trực tiếp trên Dagster user code pod (không submit Spark job)
    - Đảm bảo Python asset có thể dependency với dbt assets trong cùng asset graph
    - _Requirements: 17.1, 17.2, 17.3_

  - [x] 9.5 Tạo PipesEMRContainersClient resource (`de-team/dagster_project/resources/__init__.py`)
    - Implement `create_pipes_emr_client()` function:
      - Tạo `PipesEMRContainersClient` với `PipesS3MessageReader` (bucket, include_stdio_in_messages=True)
    - _Requirements: 10.1, 10.2_

  - [x] 9.6 Tạo Dagster Definitions (`de-team/dagster_project/definitions.py`)
    - Import tất cả assets (dbt_assets, python_assets) và resources
    - Tạo `dg.Definitions` với:
      - assets: `[de_team_dbt_assets, *python_only_assets]`
      - resources: `pipes_emr_containers_client`, `spark_config_manager` (với default SparkJobConfig chứa Iceberg + Glue properties)
    - Tạo `__init__.py` files cho tất cả packages
    - _Requirements: 1.1, 10.1_

- [x] 10. Triển khai Dagster Application Code — sales-team
  - [x] 10.1 Tạo dbt_assets.py cho sales-team (`sales-team/dagster_project/assets/dbt_assets.py`)
    - Implement `@dbt_assets` decorated function cho dbt-athena assets
    - Sử dụng `DbtCliResource` để chạy dbt trực tiếp (không cần Spark/EMR) — khác de-team
    - _Requirements: 18.3_

  - [x] 10.2 Tạo Python-only assets và resources cho sales-team
    - Tạo `sales-team/dagster_project/assets/python_assets.py` — sample Python asset
    - Tạo `sales-team/dagster_project/resources/__init__.py` — resources cho Athena
    - _Requirements: 17.1, 18.3_

  - [x] 10.3 Tạo Dagster Definitions cho sales-team (`sales-team/dagster_project/definitions.py`)
    - Import assets và resources, tạo `dg.Definitions`
    - Tạo `__init__.py` files cho tất cả packages
    - _Requirements: 18.1, 18.3_

- [x] 11. Checkpoint — Xác nhận Application Code
  - Chạy `python -c "from dagster_project.definitions import defs"` cho cả de-team và sales-team để verify import thành công
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Tạo CI/CD Pipeline — GitHub Actions
  - [x] 12.1 Tạo GitHub Actions workflow (`dbt-dagster-project/.github/workflows/ci-cd.yml`)
    - Implement workflow trigger: push to main branch, path filters cho `de-team/` và `sales-team/`
    - Implement jobs:
      - **build-de-team**: Chạy `dagster-dbt project prepare-and-package` → build Code_Image FROM Base_Image → push lên ECR với tag `git-sha`
      - **build-sales-team**: Tương tự cho sales-team
      - **update-argocd**: Update image tag trong ArgoCD App Repo (Helm values) → push commit
    - Cấu hình AWS credentials (OIDC hoặc secrets), ECR login
    - Đảm bảo chỉ rebuild code location bị thay đổi (path filter)
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 15.4, 17.4, 18.7_

- [ ] 13. Viết Property-Based Tests và Unit Tests
  - [ ]* 13.1 Viết property test cho Property 3: Spark Config Merge — Partial Override with Default Fallback
    - **Property 3: Spark Config Merge — Partial Override with Default Fallback**
    - **Validates: Requirements 2.3, 2.4, 11.2, 11.3**
    - Sử dụng Hypothesis library, tạo custom strategies:
      - `valid_spark_job_configs()` — generate random SparkJobConfig
      - `partial_spark_configs()` — generate dict với 0 đến tất cả fields
    - Test: với mọi default config D và partial meta M, merge_config(M) trả về R sao cho:
      - Field có trong M → R.field == M.field
      - Field không có trong M → R.field == D.field
      - M là None/empty → R == D
    - File: `dbt-dagster-project/tests/property/test_spark_config_properties.py`
    - Minimum 100 iterations

  - [ ]* 13.2 Viết property test cho Property 4: start_job_run_params Structure Validity
    - **Property 4: start_job_run_params Structure Validity**
    - **Validates: Requirements 3.1, 10.3**
    - Test: với mọi valid SparkJobConfig, build_start_job_run_params() trả về dict chứa đầy đủ required fields: releaseLabel, virtualClusterId, executionRoleArn, jobDriver.sparkSubmitJobDriver.entryPoint, sparkSubmitParameters chứa tất cả Spark resource configs và Iceberg catalog configs
    - File: `dbt-dagster-project/tests/property/test_spark_config_properties.py`
    - Minimum 100 iterations

  - [ ]* 13.3 Viết unit tests cho SparkConfigManager
    - Test cases: empty meta → default config; full meta → all overridden; partial meta → partial override; invalid values → ValueError
    - File: `dbt-dagster-project/tests/unit/test_spark_config.py`
    - _Requirements: 2.3, 2.4_

  - [ ]* 13.4 Viết unit tests cho entrypoint.py
    - Test cases: dbt run success → report materialization; dbt run failure → raise RuntimeError; parse run_results.json
    - File: `dbt-dagster-project/tests/unit/test_entrypoint.py`
    - _Requirements: 3.4, 3.5, 10.4_

  - [ ]* 13.5 Viết unit tests cho build_start_job_run_params
    - Test cases: verify entryPoint = "local:///app/entrypoint.py"; verify tất cả Iceberg configs present; verify Spark resource configs trong sparkSubmitParameters
    - File: `dbt-dagster-project/tests/unit/test_spark_config.py`
    - _Requirements: 10.3, 11.1_

- [ ] 14. Final checkpoint — Xác nhận toàn bộ hệ thống
  - Chạy `pytest tests/ -v` để verify tất cả tests pass
  - Chạy `terraform validate` cho mỗi module
  - Chạy `helm template` cho ArgoCD charts
  - Ensure all tests pass, ask the user if questions arise.

## Ghi chú

- Các task đánh dấu `*` là optional và có thể bỏ qua để đẩy nhanh MVP
- Mỗi task reference cụ thể requirements để đảm bảo traceability
- Checkpoints đảm bảo validation tăng dần sau mỗi phase
- Property tests validate correctness properties từ design document (Property 3, 4)
- Unit tests validate specific examples và edge cases
- Property 1 và 2 (dbt Manifest mapping, Dependency Graph) được đảm bảo bởi `dagster-dbt` library — chỉ cần integration test xác nhận cấu hình đúng
- **Terraform/Terragrunt** chỉ quản lý AWS resources (VPC, EKS, IAM, SQS, S3, ECR, Glue). Ưu tiên dùng community modules (`terraform-aws-modules/*`) ở version pinned, mỗi resource/role là 1 Terragrunt unit riêng. Chỉ viết local module khi cần gom nhiều resources (ECR 4 repos, Glue 3 databases).
- **ArgoCD** quản lý Kubernetes resources (Helm charts, CRDs, namespaces). Karpenter Helm + NodePool + EC2NodeClass nằm trong ArgoCD App Repo
