# ---------------------------------------------------------------------------------------------------------------------
# DBT PROJECT DEFINITION — CI/CD helper
# Standalone file for `dagster-dbt project prepare-and-package`.
# No relative imports — avoids "attempted relative import with no known parent package" error.
# This file is NOT used at runtime — only during CI/CD manifest generation.
# ---------------------------------------------------------------------------------------------------------------------

import os
from pathlib import Path

from dagster_dbt import DbtProject

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "dbt_project").resolve(),
    packaged_project_dir=(
        Path("/opt/dagster/app/dbt-project")
        if os.getenv("DAGSTER_ENV") == "prod"
        else None
    ),
    target=os.getenv("DBT_TARGET", None),
)
dbt_project.prepare_if_dev()
