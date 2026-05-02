# Product Overview

**lakehouse-at-scale** is a data lakehouse platform that integrates dbt with Dagster on AWS, using Apache Iceberg as the table format.

## What It Does

- Orchestrates dbt model execution as Dagster assets, with each dbt model mapped 1:1 to a Dagster asset
- Executes dbt-spark models on Apache Spark via EMR on EKS (de-team code location)
- Executes dbt-athena models via Amazon Athena (sales-team code location)
- Supports Python-only Dagster assets for lightweight tasks (API calls, validation, notifications)
- Stores data as Iceberg tables on S3 with Glue Data Catalog as the metastore
- Uses Dagster Pipes (`PipesEMRContainersClient`) for Dagster-to-Spark communication

## Key Design Decisions

- **Two code locations** (`de-team`, `sales-team`) for team isolation — independent dependencies, deployment, and failure domains
- **Base Image + Code Image** Docker pattern — Base Image has all dependencies (rarely rebuilt), Code Image only copies project code (builds in <30s)
- **dbt manifest precompiled** at CI/CD time via `dagster-dbt project prepare-and-package`, baked into Code Image
- **dbt-spark uses `method: session`** — connects directly to SparkSession in the same process on the Spark Driver Pod, no Thrift server needed
- **GitOps via ArgoCD** App-of-Apps pattern for Kubernetes deployments
- **Karpenter** for EKS node autoscaling (Spot for executors, On-Demand for drivers)

## Target Cloud

AWS — ap-southeast-1 region (non-prod). Services: EKS, EMR on EKS, S3, Glue, ECR, IAM.
