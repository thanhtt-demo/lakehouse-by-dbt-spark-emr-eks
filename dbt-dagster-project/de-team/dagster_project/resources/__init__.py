# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER RESOURCES — de-team code location
# PipesEMRContainersClient for submitting and monitoring Spark jobs on EMR on EKS.
# Uses PipesS3MessageReader to receive logs/events from Spark via S3.
# ---------------------------------------------------------------------------------------------------------------------

import boto3
from dagster_aws.pipes import PipesEMRContainersClient, PipesS3MessageReader


def create_pipes_emr_client(
    pipes_s3_bucket: str,
) -> PipesEMRContainersClient:
    """Create PipesEMRContainersClient resource.

    Uses PipesS3MessageReader to read logs/events from Spark job via S3.
    include_stdio_in_messages=True forwards Spark driver stdout/stderr to Dagster.
    """
    return PipesEMRContainersClient(
        message_reader=PipesS3MessageReader(
            client=boto3.client("s3"),
            bucket=pipes_s3_bucket,
            include_stdio_in_messages=True,
        ),
    )
