import sys
from pathlib import Path
from typing import List, Optional

import typer
from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.flags import IfNotExistsOption, OverrideableOption
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import (
    CommandResult,
    QueryJsonValueResult,
    QueryResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.util import is_valid_object_name
from snowflake.cli.plugins.object.common import CommentOption, Tag, TagOption
from snowflake.cli.plugins.spcs.common import (
    print_log_lines,
    validate_and_set_instances,
)
from snowflake.cli.plugins.spcs.services.manager import ServiceManager

app = SnowTyper(
    name="service",
    help="Manages Snowpark Container Services services.",
    short_help="Manages services.",
)


def _service_name_callback(name: str) -> str:
    if not is_valid_object_name(name, max_depth=2, allow_quoted=False):
        raise ClickException(
            f"'{name}' is not a valid service name. Note service names must be unquoted identifiers. The same constraint also applies to database and schema names where you create a service."
        )
    return name


ServiceNameArgument = typer.Argument(
    ..., help="Name of the service.", callback=_service_name_callback
)

SpecPathOption = typer.Option(
    ...,
    "--spec-path",
    help="Path to service specification file.",
    file_okay=True,
    dir_okay=False,
    exists=True,
)

_MIN_INSTANCES_HELP = "Minimum number of service instances to run."
MinInstancesOption = OverrideableOption(
    1, "--min-instances", help=_MIN_INSTANCES_HELP, min=1
)

_MAX_INSTANCES_HELP = "Maximum number of service instances to run."
MaxInstancesOption = OverrideableOption(
    None, "--max-instances", help=_MAX_INSTANCES_HELP, min=1
)

_QUERY_WAREHOUSE_HELP = "Warehouse to use if a service container connects to Snowflake to execute a query without explicitly specifying a warehouse to use."
QueryWarehouseOption = OverrideableOption(
    None,
    "--query-warehouse",
    help=_QUERY_WAREHOUSE_HELP,
)

_AUTO_RESUME_HELP = "The service will automatically resume when a service function or ingress is called."
AutoResumeOption = OverrideableOption(
    True,
    "--auto-resume/--no-auto-resume",
    help=_AUTO_RESUME_HELP,
)

_COMMENT_HELP = "Comment for the service."


@app.command(requires_connection=True)
def create(
    name: str = ServiceNameArgument,
    compute_pool: str = typer.Option(
        ..., "--compute-pool", help="Compute pool to run the service on."
    ),
    spec_path: Path = SpecPathOption,
    min_instances: int = MinInstancesOption(),
    max_instances: Optional[int] = MaxInstancesOption(),
    auto_resume: bool = AutoResumeOption(),
    external_access_integrations: Optional[List[str]] = typer.Option(
        None,
        "--eai-name",
        help="Identifies External Access Integrations(EAI) that the service can access. This option may be specified multiple times for multiple EAIs.",
    ),
    query_warehouse: Optional[str] = QueryWarehouseOption(),
    tags: Optional[List[Tag]] = TagOption(help="Tag for the service."),
    comment: Optional[str] = CommentOption(help=_COMMENT_HELP),
    if_not_exists: bool = IfNotExistsOption(),
    **options,
) -> CommandResult:
    """
    Creates a new service in the current schema.
    """
    max_instances = validate_and_set_instances(
        min_instances, max_instances, "instances"
    )
    cursor = ServiceManager(cli_context.connection).create(
        service_name=name,
        min_instances=min_instances,
        max_instances=max_instances,
        compute_pool=compute_pool,
        spec_path=spec_path,
        external_access_integrations=external_access_integrations,
        auto_resume=auto_resume,
        query_warehouse=query_warehouse,
        tags=tags,
        comment=comment,
        if_not_exists=if_not_exists,
    )
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
def status(name: str = ServiceNameArgument, **options) -> CommandResult:
    """
    Retrieves the status of a service.
    """
    cursor = ServiceManager(cli_context.connection).status(service_name=name)
    return QueryJsonValueResult(cursor)


@app.command(requires_connection=True)
def logs(
    name: str = ServiceNameArgument,
    container_name: str = typer.Option(
        ..., "--container-name", help="Name of the container."
    ),
    instance_id: str = typer.Option(
        ..., "--instance-id", help="ID of the service instance, starting with 0."
    ),
    num_lines: int = typer.Option(
        500, "--num-lines", help="Number of lines to retrieve."
    ),
    **options,
):
    """
    Retrieves local logs from a service container.
    """
    results = ServiceManager(cli_context.connection).logs(
        service_name=name,
        instance_id=instance_id,
        container_name=container_name,
        num_lines=num_lines,
    )
    cursor = results.fetchone()
    logs = next(iter(cursor)).split("\n")
    print_log_lines(sys.stdout, name, "0", logs)


@app.command(requires_connection=True)
def upgrade(
    name: str = ServiceNameArgument,
    spec_path: Path = SpecPathOption,
    **options,
):
    """
    Updates an existing service with a new specification file.
    """
    return SingleQueryResult(
        ServiceManager(cli_context.connection).upgrade_spec(
            service_name=name, spec_path=spec_path
        )
    )


@app.command("list-endpoints", requires_connection=True)
def list_endpoints(name: str = ServiceNameArgument, **options):
    """
    Lists the endpoints in a service.
    """
    return QueryResult(
        ServiceManager(cli_context.connection).list_endpoints(service_name=name)
    )


@app.command(requires_connection=True)
def suspend(name: str = ServiceNameArgument, **options) -> CommandResult:
    """
    Suspends the service, shutting down and deleting all its containers.
    """
    return SingleQueryResult(ServiceManager(cli_context.connection).suspend(name))


@app.command(requires_connection=True)
def resume(name: str = ServiceNameArgument, **options) -> CommandResult:
    """
    Resumes the service from a SUSPENDED state.
    """
    return SingleQueryResult(ServiceManager(cli_context.connection).resume(name))


@app.command("set", requires_connection=True)
def set_property(
    name: str = ServiceNameArgument,
    min_instances: Optional[int] = MinInstancesOption(default=None, show_default=False),
    max_instances: Optional[int] = MaxInstancesOption(show_default=False),
    query_warehouse: Optional[str] = QueryWarehouseOption(show_default=False),
    auto_resume: Optional[bool] = AutoResumeOption(default=None, show_default=False),
    comment: Optional[str] = CommentOption(help=_COMMENT_HELP, show_default=False),
    **options,
):
    """
    Sets one or more properties for the service.
    """
    cursor = ServiceManager(cli_context.connection).set_property(
        service_name=name,
        min_instances=min_instances,
        max_instances=max_instances,
        query_warehouse=query_warehouse,
        auto_resume=auto_resume,
        comment=comment,
    )
    return SingleQueryResult(cursor)


@app.command("unset", requires_connection=True)
def unset_property(
    name: str = ServiceNameArgument,
    min_instances: bool = MinInstancesOption(
        default=False,
        help=f"Reset the MIN_INSTANCES property - {_MIN_INSTANCES_HELP}",
        show_default=False,
    ),
    max_instances: bool = MaxInstancesOption(
        default=False,
        help=f"Reset the MAX_INSTANCES property - {_MAX_INSTANCES_HELP}",
        show_default=False,
    ),
    query_warehouse: bool = QueryWarehouseOption(
        default=False,
        help=f"Reset the QUERY_WAREHOUSE property - {_QUERY_WAREHOUSE_HELP}",
        show_default=False,
    ),
    auto_resume: bool = AutoResumeOption(
        default=False,
        param_decls=["--auto-resume"],
        help=f"Reset the AUTO_RESUME property - {_AUTO_RESUME_HELP}",
        show_default=False,
    ),
    comment: bool = CommentOption(
        default=False,
        help=f"Reset the COMMENT property - {_COMMENT_HELP}",
        callback=None,
        show_default=False,
    ),
    **options,
):
    """
    Resets one or more properties for the service to their default value(s).
    """
    cursor = ServiceManager(cli_context.connection).unset_property(
        service_name=name,
        min_instances=min_instances,
        max_instances=max_instances,
        query_warehouse=query_warehouse,
        auto_resume=auto_resume,
        comment=comment,
    )
    return SingleQueryResult(cursor)
