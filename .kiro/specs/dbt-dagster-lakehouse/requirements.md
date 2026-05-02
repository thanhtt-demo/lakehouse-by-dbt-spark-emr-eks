# Requirements Document

## Introduction

Tài liệu này mô tả các yêu cầu cho hệ thống tích hợp dbt + Dagster trên kiến trúc Lakehouse. Hệ thống cho phép mỗi dbt model được ánh xạ thành một Dagster asset, và khi materialize asset sẽ chạy dbt model trên Spark thông qua EMR on EKS. Mỗi dbt model có thể cấu hình riêng về tài nguyên driver/executor (CPU, memory, số instance). Hạ tầng được triển khai bằng Terraform + Terragrunt trên AWS, sử dụng Apache Iceberg làm table format và Glue Data Catalog cho metadata. Dagster và các platform components được deploy lên EKS thông qua ArgoCD theo mô hình GitOps (App-of-Apps pattern).

## Glossary

- **Dagster_Orchestrator**: Hệ thống Dagster chịu trách nhiệm điều phối (orchestrate) việc chạy các dbt model dưới dạng asset
- **dbt_Project**: Dự án dbt sử dụng dbt-spark adapter, chứa các model SQL/Python để transform dữ liệu
- **dbt_Model**: Một model trong dbt project, đại diện cho một phép biến đổi dữ liệu (transformation), được ánh xạ 1:1 với một Dagster asset
- **Dagster_Asset**: Một asset trong Dagster; có 2 loại: (1) dbt asset — ánh xạ từ dbt model, khi materialize sẽ submit Spark job lên EMR on EKS, (2) Python asset — code Python chạy trực tiếp trên Dagster user code pod, không cần Spark/EMR
- **EMR_Virtual_Cluster**: Amazon EMR on EKS virtual cluster, cung cấp môi trường chạy Spark job trên Kubernetes
- **EKS_Cluster**: Amazon Elastic Kubernetes Service cluster, nền tảng Kubernetes để chạy các workload
- **Spark_Job**: Một Spark job được submit lên EMR on EKS để thực thi dbt model
- **Spark_Driver_Pod**: Pod Kubernetes chạy Spark driver process
- **Spark_Executor_Pod**: Pod Kubernetes chạy Spark executor process
- **Model_Spark_Config**: Cấu hình Spark riêng cho từng dbt model, bao gồm CPU, memory, số executor instance, được định nghĩa trong YAML của dbt model
- **Iceberg_Table**: Bảng dữ liệu sử dụng Apache Iceberg table format, lưu trữ trên S3
- **Glue_Data_Catalog**: AWS Glue Data Catalog, dùng làm metastore cho Iceberg tables
- **Karpenter_Autoscaler**: Karpenter, hệ thống auto-scaling node trên EKS
- **Terragrunt_Config**: Cấu hình Terragrunt để orchestrate Terraform modules, quản lý remote state và triển khai hạ tầng theo môi trường
- **ECR_Registry**: Amazon Elastic Container Registry, lưu trữ Docker images cho Spark và dbt runtime
- **S3_Data_Lake**: Amazon S3 buckets dùng làm storage layer cho Iceberg data lake
- **Dagster_Code_Location**: Một code location trong Dagster chứa definitions (assets, resources, schedules), chạy trong pod riêng với image riêng; mỗi code location độc lập về dependencies và deployment
- **Base_Image**: Docker image chứa Python runtime và tất cả dependencies (dagster, dbt-core, dbt-spark, dagster-aws...), ít thay đổi, được IT security scan kỹ
- **Code_Image**: Docker image nhẹ, FROM Base_Image + chỉ COPY code dbt/Dagster project vào; build nhanh (<30s), không thêm packages mới nên không phát sinh vuln mới
- **Git_Code_Repo**: Git repository (GitHub) chứa code dbt project và Dagster project
- **IAM_Role**: AWS IAM role và policy để cấp quyền cho các service
- **ArgoCD**: GitOps continuous delivery tool cho Kubernetes, tự động sync trạng thái cluster với Git repository
- **App_of_Apps**: Pattern trong ArgoCD sử dụng một root Application (Helm chart) để quản lý và deploy tất cả child Applications
- **Sync_Wave**: Cơ chế trong ArgoCD để kiểm soát thứ tự deploy các resource (ví dụ: namespace trước, rồi đến application)
- **ArgoCD_App_Repo**: Git repository chứa Helm charts và Kubernetes manifests cho ArgoCD deploy, tách biệt khỏi infrastructure repo (Terragrunt)
- **PipesEMRContainersClient**: Resource có sẵn trong thư viện `dagster-aws` để launch và monitor EMR on EKS job từ Dagster asset, hỗ trợ nhận logs và events từ Spark job qua Dagster Pipes protocol
- **Dagster_Pipes**: Protocol của Dagster cho phép external compute (như Spark trên EMR) gửi logs, asset materializations, và metadata ngược về Dagster process
- **PipesS3MessageWriter**: Component trong `dagster-pipes` chạy bên trong Spark job, gửi messages (logs, events) qua S3 về Dagster
- **PipesS3MessageReader**: Component trong `dagster-aws` chạy bên Dagster, đọc messages từ S3 bucket do Spark job gửi

## Requirements

### Requirement 1: Ánh xạ dbt Model thành Dagster Asset

**User Story:** Là một data engineer, tôi muốn mỗi dbt model được tự động ánh xạ thành một Dagster asset, để tôi có thể quản lý và materialize từng model độc lập thông qua Dagster UI.

#### Acceptance Criteria

1. THE Dagster_Orchestrator SHALL load tất cả dbt model từ dbt_Project và tạo một Dagster_Asset tương ứng cho mỗi dbt_Model
2. WHEN một dbt_Model có dependency đến dbt_Model khác, THE Dagster_Orchestrator SHALL tạo dependency tương ứng giữa các Dagster_Asset
3. WHEN người dùng xem Dagster UI, THE Dagster_Orchestrator SHALL hiển thị tất cả Dagster_Asset với tên trùng khớp tên dbt_Model
4. WHEN người dùng click "Materialize" trên một Dagster_Asset, THE Dagster_Orchestrator SHALL kích hoạt chạy dbt_Model tương ứng trên Spark thông qua EMR_Virtual_Cluster

### Requirement 2: Cấu hình Spark riêng cho từng dbt Model

**User Story:** Là một data engineer, tôi muốn cấu hình tài nguyên Spark (driver/executor CPU, memory, số instance) riêng cho từng dbt model trong file YAML của model đó, để tối ưu tài nguyên cho từng workload khác nhau.

#### Acceptance Criteria

1. THE dbt_Project SHALL cho phép định nghĩa Model_Spark_Config trong meta section của mỗi dbt_Model YAML file
2. THE Model_Spark_Config SHALL bao gồm các tham số: driver CPU, driver memory, executor CPU, executor memory, và số lượng executor instance
3. WHEN một Model_Spark_Config không được định nghĩa cho một dbt_Model, THE Dagster_Orchestrator SHALL sử dụng cấu hình Spark mặc định (default config)
4. WHEN một Model_Spark_Config chỉ định nghĩa một phần tham số, THE Dagster_Orchestrator SHALL merge tham số đã định nghĩa với giá trị mặc định cho các tham số còn thiếu

### Requirement 3: Submit Spark Job lên EMR on EKS

**User Story:** Là một data engineer, tôi muốn khi materialize một Dagster asset, hệ thống tự động submit Spark job lên EMR on EKS với cấu hình đúng, để dbt model được thực thi trên Spark cluster.

#### Acceptance Criteria

1. WHEN một Dagster_Asset được materialize, THE Dagster_Orchestrator SHALL sử dụng PipesEMRContainersClient để submit một Spark_Job lên EMR_Virtual_Cluster với Model_Spark_Config tương ứng
2. WHEN Spark_Job được submit, THE EMR_Virtual_Cluster SHALL tạo Spark_Driver_Pod và Spark_Executor_Pod trên EKS_Cluster theo cấu hình trong Model_Spark_Config
3. WHILE Spark_Job đang chạy, THE PipesEMRContainersClient SHALL tự động theo dõi trạng thái job và forward logs từ Spark driver về Dagster
4. WHEN Spark_Job hoàn thành thành công, THE Dagster_Orchestrator SHALL đánh dấu Dagster_Asset là materialized thành công với metadata từ Spark job
5. IF Spark_Job thất bại, THEN THE PipesEMRContainersClient SHALL raise exception và Dagster_Orchestrator đánh dấu Dagster_Asset là failed

### Requirement 4: Iceberg Table Format và Data Lake Storage

**User Story:** Là một data engineer, tôi muốn dữ liệu được lưu trữ dưới dạng Iceberg table trên S3, để tận dụng các tính năng ACID transactions, time travel và schema evolution của Iceberg.

#### Acceptance Criteria

1. THE dbt_Project SHALL cấu hình dbt-spark adapter để sử dụng Apache Iceberg làm table format mặc định
2. WHEN một dbt_Model được materialize, THE Spark_Job SHALL ghi dữ liệu output dưới dạng Iceberg_Table trên S3_Data_Lake
3. THE Spark_Job SHALL đăng ký metadata của Iceberg_Table vào Glue_Data_Catalog
4. WHEN một dbt_Model sử dụng incremental materialization, THE Spark_Job SHALL thực hiện merge operation trên Iceberg_Table

### Requirement 5: EKS Cluster và Karpenter Auto-scaling

**User Story:** Là một platform engineer, tôi muốn EKS cluster tự động scale node dựa trên workload Spark, để tối ưu chi phí và đảm bảo đủ tài nguyên khi chạy nhiều dbt model đồng thời.

#### Acceptance Criteria

1. THE EKS_Cluster SHALL được triển khai với Karpenter_Autoscaler để tự động provision node
2. WHEN Spark_Driver_Pod hoặc Spark_Executor_Pod không thể schedule do thiếu tài nguyên, THE Karpenter_Autoscaler SHALL tự động provision node mới trong vòng 120 giây
3. WHEN không còn Spark pod nào chạy trên một node, THE Karpenter_Autoscaler SHALL tự động terminate node đó sau thời gian chờ cấu hình được (configurable TTL)
4. THE Karpenter_Autoscaler SHALL hỗ trợ cấu hình instance type constraints (ví dụ: chỉ dùng Spot instance cho executor, On-Demand cho driver)

### Requirement 6: Hạ tầng Terraform + Terragrunt

**User Story:** Là một platform engineer, tôi muốn toàn bộ hạ tầng AWS được định nghĩa bằng Terraform modules và orchestrate bằng Terragrunt, để đảm bảo infrastructure-as-code và dễ dàng triển khai qua nhiều môi trường.

#### Acceptance Criteria

1. THE Terragrunt_Config SHALL tổ chức theo cấu trúc thư mục: `_envcommon/` cho shared configs, `modules/` cho Terraform modules, và `non-prod/{account}/{region}/{service}/` cho từng service
2. THE Terragrunt_Config SHALL sử dụng S3 bucket cho remote state và DynamoDB table cho state locking
3. THE Terragrunt_Config SHALL sử dụng file `env.hcl` để định nghĩa biến môi trường (aws_account_id, aws_profile) cho từng environment
4. THE Terragrunt_Config SHALL bao gồm Terraform modules cho: VPC, EKS_Cluster, EMR_Virtual_Cluster, ECR_Registry, S3_Data_Lake, IAM_Role, và Glue_Data_Catalog
5. WHEN triển khai một module, THE Terragrunt_Config SHALL tự động resolve dependency giữa các module (ví dụ: EKS phụ thuộc VPC)

### Requirement 7: EMR Virtual Cluster trên EKS

**User Story:** Là một platform engineer, tôi muốn EMR Virtual Cluster được cấu hình trên EKS cluster, để có thể submit Spark job mà không cần quản lý Spark cluster riêng.

#### Acceptance Criteria

1. THE EMR_Virtual_Cluster SHALL được tạo trên EKS_Cluster với namespace riêng cho Spark workload
2. THE EMR_Virtual_Cluster SHALL sử dụng IAM_Role với quyền đọc/ghi S3_Data_Lake và truy cập Glue_Data_Catalog
3. WHEN một Spark_Job được submit, THE EMR_Virtual_Cluster SHALL sử dụng Docker image từ ECR_Registry chứa Spark runtime và dbt
4. THE EMR_Virtual_Cluster SHALL hỗ trợ cấu hình Spark properties mặc định (spark.sql.catalog, spark.sql.extensions) cho Iceberg integration

### Requirement 8: Docker Images — Base Image + Code Image Pattern

**User Story:** Là một data engineer, tôi muốn Docker image được tách thành base image (runtime + dependencies) và code image (chỉ COPY code), để khi thay đổi code dbt/Dagster chỉ cần build code image nhẹ mà không phát sinh vulnerability mới.

#### Acceptance Criteria

1. THE ECR_Registry SHALL chứa Base_Image bao gồm: Apache Spark runtime, Python runtime, dbt-core, dbt-spark adapter, dagster-pipes, boto3, Iceberg Spark runtime JAR, và tất cả Python dependencies — KHÔNG chứa code project
2. THE Base_Image SHALL được build và push lên ECR_Registry thông qua Terraform module (docker-image module), chỉ rebuild khi thay đổi dependency versions
3. THE ECR_Registry SHALL chứa Code_Image cho mỗi Dagster_Code_Location, được build `FROM Base_Image` và chỉ `COPY` code dbt/Dagster project vào — KHÔNG cài thêm packages
4. THE Code_Image SHALL build nhanh (<30 giây) vì chỉ thêm 1 layer COPY code, và SHALL KHÔNG phát sinh vulnerability mới so với Base_Image
5. WHEN code dbt hoặc Dagster thay đổi, THE CI/CD pipeline SHALL chỉ cần rebuild Code_Image và push lên ECR_Registry

### Requirement 13: Deployment Flow — CI/CD + ArgoCD Auto-sync

**User Story:** Là một data engineer, tôi muốn khi push code mới lên GitHub, CI/CD tự động build code image và ArgoCD tự động deploy lên EKS, để quá trình deployment hoàn toàn tự động mà không cần thao tác thủ công.

#### Acceptance Criteria

1. WHEN code dbt_Project hoặc Dagster code được push lên Git_Code_Repo, THE CI/CD pipeline (GitHub Actions) SHALL tự động build Code_Image mới từ Base_Image và push lên ECR_Registry với tag mới (ví dụ: git commit SHA)
2. THE CI/CD pipeline SHALL update image tag trong ArgoCD_App_Repo (Helm values) và push commit
3. THE ArgoCD SHALL detect thay đổi image tag và tự động sync — rolling update Dagster user code pod với Code_Image mới
4. WHEN Dagster user code pod restart với Code_Image mới, THE Dagster_Orchestrator SHALL tự động re-parse dbt manifest và cập nhật tất cả Dagster_Asset
5. THE Spark_Job Docker image (cho EMR on EKS) SHALL cũng sử dụng pattern Base_Image + Code_Image, với dbt project code được COPY vào image
6. THE toàn bộ flow từ git push đến Dagster cập nhật assets SHALL hoàn thành trong vòng 5 phút

### Requirement 9: IAM Roles và Security

**User Story:** Là một platform engineer, tôi muốn mỗi component có IAM role riêng với quyền tối thiểu (least privilege), để đảm bảo bảo mật cho hệ thống.

#### Acceptance Criteria

1. THE IAM_Role cho EMR_Virtual_Cluster SHALL chỉ có quyền đọc/ghi vào S3_Data_Lake buckets được chỉ định
2. THE IAM_Role cho EMR_Virtual_Cluster SHALL chỉ có quyền truy cập Glue_Data_Catalog databases được chỉ định
3. THE IAM_Role cho Dagster_Orchestrator SHALL chỉ có quyền submit và monitor Spark_Job trên EMR_Virtual_Cluster
4. THE IAM_Role cho Karpenter_Autoscaler SHALL chỉ có quyền provision và terminate EC2 instances trong EKS_Cluster

### Requirement 10: Dagster Pipes EMR Containers Integration

**User Story:** Là một data engineer, tôi muốn sử dụng PipesEMRContainersClient có sẵn trong dagster-aws để submit và quản lý Spark job trên EMR on EKS, để tận dụng Dagster Pipes protocol nhận logs và events từ Spark job.

#### Acceptance Criteria

1. THE Dagster_Orchestrator SHALL sử dụng PipesEMRContainersClient từ thư viện `dagster-aws` làm resource để submit Spark_Job lên EMR_Virtual_Cluster
2. THE PipesEMRContainersClient SHALL được cấu hình với PipesS3MessageReader để đọc logs và events từ Spark job qua S3 bucket
3. WHEN submit Spark_Job, THE Dagster_Asset SHALL truyền `start_job_run_params` bao gồm: releaseLabel, virtualClusterId, executionRoleArn, và jobDriver với sparkSubmitJobDriver chứa entryPoint và Model_Spark_Config
4. THE Spark_Job script SHALL sử dụng `open_dagster_pipes` với PipesS3MessageWriter để gửi logs, asset materializations, và metadata ngược về Dagster
5. THE PipesEMRContainersClient SHALL tự động theo dõi trạng thái job và raise exception nếu job thất bại
6. THE Docker image cho Spark SHALL bao gồm packages `dagster-pipes` và `boto3` để hỗ trợ Dagster Pipes protocol

### Requirement 11: Spark Job Template

**User Story:** Là một data engineer, tôi muốn có Spark job template chuẩn cho EMR on EKS, để đảm bảo mọi dbt model đều chạy với cấu hình Iceberg và Glue Catalog đúng.

#### Acceptance Criteria

1. THE Spark_Job template SHALL bao gồm cấu hình mặc định cho Iceberg: spark.sql.catalog.glue_catalog, spark.sql.catalog.glue_catalog.warehouse, spark.sql.extensions
2. THE Spark_Job template SHALL cho phép override cấu hình mặc định bằng Model_Spark_Config từ dbt_Model
3. WHEN tạo start_job_run_params, THE Dagster_Asset SHALL merge Spark_Job template với Model_Spark_Config, trong đó Model_Spark_Config có độ ưu tiên cao hơn
4. THE Spark_Job template SHALL cấu hình log4j để ghi log Spark vào S3_Data_Lake hoặc CloudWatch

### Requirement 12: Cấu hình dbt-spark cho Iceberg

**User Story:** Là một data engineer, tôi muốn dbt project được cấu hình sẵn để sử dụng dbt-spark adapter với Iceberg table format, để tôi chỉ cần viết SQL model mà không cần lo cấu hình Spark/Iceberg.

#### Acceptance Criteria

1. THE dbt_Project SHALL cấu hình profiles.yml với dbt-spark adapter sử dụng `method: session` — kết nối trực tiếp đến SparkSession active trong cùng Python process trên Spark Driver Pod (EMR on EKS). Không cần Thrift server.
2. THE dbt_Project SHALL cấu hình dbt_project.yml với materialization mặc định là "table" với file_format "iceberg"
3. WHEN một dbt_Model sử dụng materialization "incremental", THE dbt_Project SHALL cấu hình incremental_strategy phù hợp với Iceberg (merge hoặc append)
4. THE dbt_Project SHALL cấu hình schema mapping để dbt schema tương ứng với Glue_Data_Catalog database

### Requirement 14: ArgoCD GitOps Deployment

**User Story:** Là một platform engineer, tôi muốn deploy Dagster và các platform components lên EKS thông qua ArgoCD theo mô hình GitOps, để mọi thay đổi deployment đều được quản lý qua Git và tự động sync.

#### Acceptance Criteria

1. THE EKS_Cluster SHALL có ArgoCD được cài đặt (thông qua Terraform/Terragrunt hoặc bootstrap manifest)
2. THE ArgoCD SHALL sử dụng App_of_Apps pattern với một root Application (Helm chart) để quản lý tất cả child Applications
3. THE ArgoCD_App_Repo SHALL có cấu trúc thư mục theo pattern: `apps/` cho root Helm chart, `dagster/` cho Dagster umbrella chart, `namespaces/` cho namespace manifests
4. THE ArgoCD SHALL deploy các component theo thứ tự Sync_Wave: namespaces (wave 1) → Dagster (wave 2) → các service phụ trợ (wave 3+)
5. THE ArgoCD SHALL được cấu hình auto-sync và self-heal để tự động reconcile drift giữa Git và cluster state

### Requirement 15: Deploy Dagster qua ArgoCD Helm Chart

**User Story:** Là một platform engineer, tôi muốn Dagster được deploy lên EKS dưới dạng Helm umbrella chart qua ArgoCD, để dễ dàng quản lý version, cấu hình, và rollback.

#### Acceptance Criteria

1. THE ArgoCD_App_Repo SHALL chứa một Dagster umbrella Helm chart (thư mục `dagster/`) wrap official Dagster Helm chart làm dependency
2. THE Dagster Helm chart SHALL cấu hình Dagster webserver, daemon, và 2 user code deployments (2 code locations) trên EKS_Cluster
3. THE Dagster Helm chart values SHALL cho phép cấu hình cho mỗi code location: Code_Image repository/tag, resource requests/limits, environment variables, và service account
4. WHEN code Dagster thay đổi, THE CI/CD pipeline SHALL rebuild Code_Image tương ứng, update image tag trong ArgoCD_App_Repo, và ArgoCD tự động sync — chỉ redeploy code location bị thay đổi
5. THE Dagster deployment SHALL sử dụng Kubernetes service account với IAM_Role (IRSA) để có quyền submit Spark_Job lên EMR_Virtual_Cluster

### Requirement 16: ArgoCD App-of-Apps Helm Chart

**User Story:** Là một platform engineer, tôi muốn có một root Helm chart quản lý tất cả ArgoCD Applications từ một file values.yaml duy nhất, để dễ dàng thêm/bớt component và kiểm soát deployment.

#### Acceptance Criteria

1. THE `apps/` Helm chart SHALL có templates tạo ArgoCD Application CRD cho mỗi entry trong `values.yaml`
2. THE `apps/values.yaml` SHALL là single source of truth cho tất cả applications: tên, repo URL, path, target namespace, sync wave, và Helm values
3. THE `apps/` Helm chart SHALL tạo ArgoCD AppProject với quyền truy cập chỉ các namespace được chỉ định
4. WHEN cần thêm một component mới (ví dụ: monitoring), THE platform engineer SHALL chỉ cần thêm entry vào `apps/values.yaml` và push Git — ArgoCD tự động deploy

### Requirement 17: Python-only Dagster Assets

**User Story:** Là một data engineer, tôi muốn có thể viết Dagster asset bằng Python thuần (không cần Spark/EMR), chạy trực tiếp trên Dagster user code pod, để xử lý các task nhẹ như API calls, data validation, hoặc notification.

#### Acceptance Criteria

1. THE Dagster_Orchestrator SHALL hỗ trợ Python-only Dagster_Asset chạy trực tiếp trên Dagster user code pod, KHÔNG submit Spark_Job lên EMR_Virtual_Cluster
2. THE Python-only Dagster_Asset code SHALL nằm trong Code_Image của Dagster_Code_Location tương ứng
3. THE Python-only Dagster_Asset SHALL có thể dependency với dbt Dagster_Asset và ngược lại trong cùng asset graph
4. WHEN code Python asset thay đổi, THE CI/CD pipeline SHALL rebuild Code_Image và ArgoCD tự động deploy — giống flow của dbt assets

### Requirement 18: Demo 2 Code Locations theo Team

**User Story:** Là một platform engineer, tôi muốn demo hệ thống với 2 Dagster code locations chia theo team (de-team dùng dbt-spark, sales-team dùng dbt-athena), để chứng minh khả năng isolation — mỗi team có dependencies riêng, deploy độc lập, lỗi không ảnh hưởng lẫn nhau.

#### Acceptance Criteria

1. THE Dagster_Orchestrator SHALL được cấu hình với 2 Dagster_Code_Location chia theo team: `de-team` và `sales-team`, mỗi location có Code_Image riêng, pod riêng, dependencies riêng
2. THE code location `de-team` SHALL sử dụng dbt-spark adapter, chứa dbt assets submit Spark job lên EMR on EKS qua PipesEMRContainersClient, và Python-only assets chạy trực tiếp trên user code pod
3. THE code location `sales-team` SHALL sử dụng dbt-athena adapter, chứa dbt assets chạy query trên Amazon Athena (không cần Spark/EMR), đọc/ghi Iceberg tables trên cùng S3_Data_Lake
4. THE Base_Image cho `de-team` SHALL chứa dbt-spark, dagster-aws (PipesEMRContainersClient), và Spark-related dependencies
5. THE Base_Image cho `sales-team` SHALL chứa dbt-athena, dagster-aws, và Athena-related dependencies — KHÁC dependencies với `de-team`
6. WHEN code location `de-team` gặp lỗi (ví dụ: import error do dbt-spark), THE code location `sales-team` SHALL vẫn hoạt động bình thường và ngược lại
7. THE 2 code locations SHALL có thể được deploy và update độc lập — update 1 team KHÔNG trigger redeploy team còn lại
8. THE Dagster Helm chart SHALL cấu hình `userCodeDeployments` với 2 entries, mỗi entry trỏ đến Code_Image riêng của từng team
