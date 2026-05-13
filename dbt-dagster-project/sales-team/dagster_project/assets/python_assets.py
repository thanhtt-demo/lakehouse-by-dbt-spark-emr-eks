# ---------------------------------------------------------------------------------------------------------------------
# PYTHON-ONLY ASSETS — sales-team
# Lightweight assets that run directly on the Dagster user code pod (no Spark/EMR, no Athena).
# Can depend on dbt assets and vice versa within the same asset graph.
# ---------------------------------------------------------------------------------------------------------------------

import dagster as dg


@dg.asset(
    deps=["stg_sales"],
    description=(
        "Validates the staged sales data after materialization. "
        "Runs directly on the Dagster user code pod — no Athena query needed."
    ),
    group_name="raw",
)
def sales_data_validation(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Sample Python-only asset that validates staged sales data.

    Demonstrates:
    - Dependency on a dbt asset (stg_sales) within the same asset graph
    - Running lightweight logic directly on the Dagster pod
    - Returning structured metadata as MaterializeResult
    """
    context.log.info("Running post-materialization validation for stg_sales")

    # Placeholder validation logic — in production this could:
    # - Query Athena to check row counts and freshness
    # - Verify Glue Data Catalog registration
    # - Send Slack/email notifications
    validation_checks = {
        "schema_exists": True,
        "table_registered_in_glue": True,
    }

    return dg.MaterializeResult(
        metadata={
            "validation_checks": len(validation_checks),
            "all_passed": all(validation_checks.values()),
        },
    )


# Collect all Python-only assets for easy import in definitions.py
python_only_assets = [sales_data_validation]
