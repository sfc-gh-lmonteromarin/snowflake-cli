import sys
from pathlib import Path

import typer
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, SingleQueryResult
from snowflake.cli.plugins.spcs.common import print_log_lines
from snowflake.cli.plugins.spcs.jobs.manager import JobManager

app = SnowTyper(
    name="job",
    help="Manages Snowpark jobs.",
    hidden=True,
)


@app.command(requires_connection=True)
def create(
    compute_pool: str = typer.Option(
        ..., "--compute-pool", help="Name of the pool in which to run the job."
    ),
    spec_path: Path = typer.Option(
        ...,
        "--spec-path",
        help="Path to the `spec.yaml` file containing the job details.",
        file_okay=True,
        dir_okay=False,
        exists=True,
    ),
    **options,
) -> CommandResult:
    """
    Creates a job to run in a compute pool.
    """
    cursor = JobManager(cli_context).create(
        compute_pool=compute_pool, spec_path=spec_path
    )
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
def logs(
    identifier: str = typer.Argument(..., help="Job id"),
    container_name: str = typer.Option(
        ..., "--container-name", help="Name of the container."
    ),
    **options,
):
    """
    Retrieves local logs from a job container.
    """
    results = JobManager(cli_context.connection).logs(
        job_name=identifier, container_name=container_name
    )
    cursor = results.fetchone()
    logs = next(iter(cursor)).split("\n")
    print_log_lines(sys.stdout, identifier, "0", logs)


@app.command(requires_connection=True)
def status(
    identifier: str = typer.Argument(..., help="ID of the job."), **options
) -> CommandResult:
    """
    Returns the status of a named Snowpark Container Services job.
    """
    cursor = JobManager(cli_context.connection).status(job_name=identifier)
    return SingleQueryResult(cursor)
