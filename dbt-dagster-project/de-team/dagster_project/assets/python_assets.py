# ---------------------------------------------------------------------------------------------------------------------
# PYTHON-ONLY ASSETS — de-team
# Lightweight assets that run directly on the Dagster user code pod (no Spark/EMR).
# Can depend on dbt assets and vice versa within the same asset graph.
# ---------------------------------------------------------------------------------------------------------------------

import dagster as dg


@dg.asset(
    deps=["orders"],
    description=(
        "Validates the orders mart after materialization. "
        "Runs directly on the Dagster user code pod — no Spark job needed."
    ),
    group_name="de_team",
)
def orders_validation(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Sample Python-only asset that validates the orders mart.

    Demonstrates:
    - Dependency on a dbt asset (orders) within the same asset graph
    - Running lightweight logic directly on the Dagster pod
    - Returning structured metadata as MaterializeResult
    """
    context.log.info("Running post-materialization validation for orders mart")

    # Placeholder validation logic — in production this could:
    # - Query Athena/Glue to check row counts
    # - Verify partition freshness
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
python_only_assets = [orders_validation]
