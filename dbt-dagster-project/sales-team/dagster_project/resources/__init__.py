# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER RESOURCES — sales-team code location
# DbtCliResource for running dbt commands directly on the Dagster user code pod.
# No Spark/EMR needed — dbt-athena queries execute on Amazon Athena.
# ---------------------------------------------------------------------------------------------------------------------

from dagster_dbt import DbtCliResource


def create_dbt_cli_resource(project_dir: str, profiles_dir: str) -> DbtCliResource:
    """Create DbtCliResource for running dbt-athena commands.

    dbt runs directly on the Dagster user code pod. Queries are submitted to
    Amazon Athena — no Spark or EMR infrastructure required.

    Args:
        project_dir: Path to the dbt project directory.
        profiles_dir: Path to the directory containing profiles.yml.
    """
    return DbtCliResource(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )
