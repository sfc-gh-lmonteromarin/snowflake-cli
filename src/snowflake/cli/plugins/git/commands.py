import logging
from pathlib import Path

import typer
from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.flags import (
    PatternOption,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.output.types import CommandResult, QueryResult
from snowflake.cli.api.utils.path_utils import is_stage_path
from snowflake.cli.plugins.git.manager import GitManager
from snowflake.cli.plugins.object.manager import ObjectManager

app = SnowTyper(
    name="git",
    help="Manages git repositories in Snowflake.",
)
log = logging.getLogger(__name__)


def _repo_path_argument_callback(path):
    # All repository paths must start with repository scope:
    # "@repo_name/tag/example_tag/*"
    if not is_stage_path(path) or path.count("/") < 3:
        raise ClickException(
            "REPOSITORY_PATH should be a path to git repository stage with scope provided."
            " Path to the repository root must end with '/'."
            " For example: @my_repo/branches/main/"
        )

    return path


RepoNameArgument = identifier_argument(sf_object="git repository", example="my_repo")
RepoPathArgument = typer.Argument(
    metavar="REPOSITORY_PATH",
    help=(
        "Path to git repository stage with scope provided."
        " Path to the repository root must end with '/'."
        " For example: @my_repo/branches/main/"
    ),
    callback=_repo_path_argument_callback,
)


def _assure_repository_does_not_exist(om: ObjectManager, repository_name: str) -> None:
    if om.object_exists(
        object_type=ObjectType.GIT_REPOSITORY.value.cli_name, name=repository_name
    ):
        raise ClickException(f"Repository '{repository_name}' already exists")


def _validate_origin_url(url: str) -> None:
    if not url.startswith("https://"):
        raise ClickException("Url address should start with 'https'")


@app.command("setup", requires_connection=True)
def setup(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    """
    Sets up a git repository object.

    You will be prompted for:

    * url - address of repository to be used for git clone operation

    * secret - Snowflake secret containing authentication credentials. Not needed if origin repository does not require
    authentication for RO operations (clone, fetch)

    * API integration - object allowing Snowflake to interact with git repository.
    """
    manager = GitManager(cli_context.connection)
    om = ObjectManager(cli_context.connection)
    _assure_repository_does_not_exist(om, repository_name)

    url = typer.prompt("Origin url")
    _validate_origin_url(url)

    secret_needed = typer.confirm("Use secret for authentication?")
    should_create_secret = False
    secret_name = None
    if secret_needed:
        secret_name = f"{repository_name}_secret"
        secret_name = typer.prompt(
            "Secret identifier (will be created if not exists)", default=secret_name
        )
        if om.object_exists(
            object_type=ObjectType.SECRET.value.cli_name, name=secret_name
        ):
            cli_console.step(f"Using existing secret '{secret_name}'")
        else:
            should_create_secret = True
            cli_console.step(f"Secret '{secret_name}' will be created")
            secret_username = typer.prompt("username")
            secret_password = typer.prompt("password/token", hide_input=True)

    api_integration = f"{repository_name}_api_integration"
    api_integration = typer.prompt(
        "API integration identifier (will be created if not exists)",
        default=api_integration,
    )

    if should_create_secret:
        manager.create_password_secret(
            name=secret_name, username=secret_username, password=secret_password
        )
        cli_console.step(f"Secret '{secret_name}' successfully created.")

    if not om.object_exists(
        object_type=ObjectType.INTEGRATION.value.cli_name, name=api_integration
    ):
        manager.create_api_integration(
            name=api_integration,
            api_provider="git_https_api",
            allowed_prefix=url,
            secret=secret_name,
        )
        cli_console.step(f"API integration '{api_integration}' successfully created.")
    else:
        cli_console.step(f"Using existing API integration '{api_integration}'.")

    return QueryResult(
        manager.create(
            repo_name=repository_name,
            url=url,
            api_integration=api_integration,
            secret=secret_name,
        )
    )


@app.command(
    "list-branches",
    requires_connection=True,
)
def list_branches(
    repository_name: str = RepoNameArgument,
    like=like_option(
        help_example='`list-branches --like "%_test"` lists all branches that end with "_test"'
    ),
    **options,
) -> CommandResult:
    """
    List all branches in the repository.
    """
    return QueryResult(
        GitManager(cli_context.connection).show_branches(
            repo_name=repository_name, like=like
        )
    )


@app.command(
    "list-tags",
    requires_connection=True,
)
def list_tags(
    repository_name: str = RepoNameArgument,
    like=like_option(
        help_example='`list-tags --like "v2.0%"` lists all tags that start with "v2.0"'
    ),
    **options,
) -> CommandResult:
    """
    List all tags in the repository.
    """
    return QueryResult(
        GitManager(cli_context.connection).show_tags(
            repo_name=repository_name, like=like
        )
    )


@app.command(
    "list-files",
    requires_connection=True,
)
def list_files(
    repository_path: str = RepoPathArgument,
    pattern=PatternOption,
    **options,
) -> CommandResult:
    """
    List files from given state of git repository.
    """
    return QueryResult(
        GitManager(cli_context.connection).list_files(
            stage_name=repository_path, pattern=pattern
        )
    )


@app.command(
    "fetch",
    requires_connection=True,
)
def fetch(
    repository_name: str = RepoNameArgument,
    **options,
) -> CommandResult:
    """
    Fetch changes from origin to snowflake repository.
    """
    return QueryResult(
        GitManager(cli_context.connection).fetch(repo_name=repository_name)
    )


@app.command(
    "copy",
    requires_connection=True,
)
def copy(
    repository_path: str = RepoPathArgument,
    destination_path: str = typer.Argument(
        help="Target path for copy operation. Should be a path to a directory on remote stage or local file system.",
    ),
    parallel: int = typer.Option(
        4,
        help="Number of parallel threads to use when downloading files.",
    ),
    **options,
):
    """
    Copies all files from given state of repository to local directory or stage.

    If the source path ends with '/', the command copies contents of specified directory.
    Otherwise, it creates a new directory or file in the destination directory.
    """
    is_copy = is_stage_path(destination_path)
    if is_copy:
        cursor = GitManager(cli_context.connection).copy_files(
            source_path=repository_path, destination_path=destination_path
        )
    else:
        cursor = GitManager(cli_context.connection).get(
            stage_path=repository_path,
            dest_path=Path(destination_path).resolve(),
            parallel=parallel,
        )
    return QueryResult(cursor)
