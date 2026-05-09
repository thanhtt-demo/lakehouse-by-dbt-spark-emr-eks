# ---------------------------------------------------------------------------------------------------------------------
# DBT ASSETS — sales-team (dbt-athena on Amazon Athena)
# Each dbt model maps 1:1 to a Dagster asset. Unlike de-team, execution runs directly on the
# Dagster user code pod via DbtCliResource — no Spark/EMR needed. Queries execute on Athena.
# ---------------------------------------------------------------------------------------------------------------------

import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets


# ---------------------------------------------------------------------------------------------------------------------
# DBT PROJECT SETUP
# Dev: generate manifest at runtime via prepare_if_dev()
# Prod: manifest precompiled in Code_Image via `dagster-dbt project prepare-and-package`
# ---------------------------------------------------------------------------------------------------------------------

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "dbt_project").resolve(),
    packaged_project_dir=(
        Path("/opt/dagster/app/dbt-project")
        if os.getenv("DAGSTER_ENV") == "prod"
        else None
    ),
)
if os.getenv("DAGSTER_ENV") != "prod":
    dbt_project.prepare_if_dev()


# ---------------------------------------------------------------------------------------------------------------------
# DBT ASSETS DEFINITION
# Each dbt model becomes a Dagster asset. On materialize, dbt CLI runs directly on the pod
# using DbtCliResource. dbt-athena submits queries to Amazon Athena.
# dbt tests automatically become Dagster asset checks (dagster-dbt 0.23.0+).
# ---------------------------------------------------------------------------------------------------------------------

@dbt_assets(
    manifest=dbt_project.manifest_path,
)
def sales_team_dbt_assets(
    context: AssetExecutionContext,
    dbt_cli: DbtCliResource,
):
    """dbt assets for sales-team — each model runs via dbt-athena on Amazon Athena.

    Key difference from de-team:
    - Uses DbtCliResource to run dbt directly on the Dagster pod
    - No Spark/EMR — queries execute on Amazon Athena
    - dbt.cli().stream() yields events (materializations + asset checks) directly
    """
    yield from dbt_cli.cli(["--debug", "build"], context=context).stream()
