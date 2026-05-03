# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER DEFINITIONS — sales-team code location
# Registers all assets (dbt + Python-only) and resources (DbtCliResource).
# This is the entry point that Dagster loads for the sales-team code location.
# Key difference from de-team: uses DbtCliResource (dbt-athena) instead of PipesEMRContainersClient.
# ---------------------------------------------------------------------------------------------------------------------

import dagster as dg

from .assets.dbt_assets import dbt_project, sales_team_dbt_assets
from .assets.python_assets import python_only_assets
from .resources import create_dbt_cli_resource


# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER DEFINITIONS
# ---------------------------------------------------------------------------------------------------------------------

defs = dg.Definitions(
    assets=[sales_team_dbt_assets, *python_only_assets],
    resources={
        "dbt_cli": create_dbt_cli_resource(
            project_dir=str(dbt_project.project_dir),
            profiles_dir=str(dbt_project.project_dir),
        ),
    },
)
