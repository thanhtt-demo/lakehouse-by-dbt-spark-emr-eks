# ---------------------------------------------------------------------------------------------------------------------
# PYTHON-ONLY ASSETS — de-team
# Lightweight assets that run directly on the Dagster user code pod (no Spark/EMR).
# Can depend on dbt assets and vice versa within the same asset graph.
# ---------------------------------------------------------------------------------------------------------------------

import dagster as dg


@dg.asset(
    deps=["orders", "customer_orders"],
    description=(
        "Extracts orders and customer_orders marts to CSV files and uploads to SFTP server. "
        "Runs directly on the Dagster user code pod — no Spark job needed."
    ),
    group_name="sftp",
    kinds={"python"},
)
def orders_files(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Extract orders data to SFTP.

    Simulates:
    - Reading from Iceberg tables (orders + customer_orders marts)
    - Writing CSV files to a local staging directory
    - Uploading files to an SFTP server for downstream consumers
    """
    import datetime

    context.log.info("Connecting to SFTP server sftp.partner.example.com:22")

    # Simulated SFTP upload logic
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    files_uploaded = [
        f"/outbound/orders_{timestamp}.csv",
        f"/outbound/customer_orders_{timestamp}.csv",
    ]

    context.log.info(f"Uploading {len(files_uploaded)} files to SFTP...")
    for file_path in files_uploaded:
        context.log.info(f"  Uploaded: {file_path}")

    context.log.info("SFTP upload complete")

    return dg.MaterializeResult(
        metadata={
            "files_uploaded": len(files_uploaded),
            "sftp_host": "sftp.partner.example.com",
            "remote_paths": files_uploaded,
            "timestamp": timestamp,
        },
    )


# Collect all Python-only assets for easy import in definitions.py
python_only_assets = [orders_files]
