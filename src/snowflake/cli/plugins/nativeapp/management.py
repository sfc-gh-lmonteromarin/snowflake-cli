from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import jinja2
from snowflake.cli.api.console import cli_console as cc  # TODO: Modify this
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.util import (
    unquote_identifier,
)
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.connection.util import make_snowsight_url
from snowflake.cli.plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    ERROR_MESSAGE_606,
    ERROR_MESSAGE_2043,
    INTERNAL_DISTRIBUTION,
    NAME_COL,
    OWNER_COL,
    SPECIAL_COMMENT,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    InvalidPackageScriptError,
    MissingPackageScriptError,
    UnexpectedOwnerError,
)
from snowflake.cli.plugins.object.stage.diff import (
    DiffResult,
    stage_diff,
    sync_local_diff_with_stage,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.connection import SnowflakeConnection


class SimpleExecuter(SqlExecutionMixin):  # TODO: Move this somewhere else
    def __init__(self, custom_connection: SnowflakeConnection):
        self.custom_connection = custom_connection

    @property
    def _conn(self):
        return self.custom_connection

    def execute_query(self, query: str, **kwargs):
        return self._execute_query(query, **kwargs)

    def execute_query_with_role(self, query: str, role: str, **kwargs):
        with self.use_role(role):
            return self._execute_query(query, **kwargs)

    def execute_queries_with_role(self, queries: str, role: str, **kwargs):
        with self.use_role(role):
            return self._execute_queries(queries, **kwargs)

    def show_specific_object_with_role(
        self,
        role: str,
        object_type_plural: str,
        name: str,
        name_col: str = "name",
        in_clause: str = "",
        check_schema: bool = False,
    ) -> Optional[dict]:
        with self.use_role(role):
            return self.show_specific_object(
                object_type_plural, name, name_col, in_clause, check_schema
            )


def execute_query(conn: SnowflakeConnection, query: str, **kwargs):
    executer = SimpleExecuter(conn)
    return executer.execute_query(query, **kwargs)


def execute_query_with_role(conn: SnowflakeConnection, role: str, query: str, **kwargs):
    executer = SimpleExecuter(conn)
    return executer.execute_query_with_role(query, role, **kwargs)


def execute_queries_with_role(
    conn: SnowflakeConnection, role: str, queries: str, **kwargs
):
    executer = SimpleExecuter(conn)
    return executer.execute_queries_with_role(queries, role, **kwargs)


def show_specific_object_with_role(
    conn: SnowflakeConnection,
    role: str,
    object_type_plural: str,
    name: str,
    name_col: str = "name",
    in_clause: str = "",
    check_schema: bool = False,
) -> Optional[dict]:
    executer = SimpleExecuter(conn)
    return executer.show_specific_object_with_role(
        role, object_type_plural, name, name_col, in_clause, check_schema
    )


# TODO: LOGIC for manager begins here


def get_app_pkg_distribution_in_snowflake(
    conn: SnowflakeConnection,  # TODO: Review all (external) references
    package_name: str,
    package_role: str,
) -> str:  # TODO: This was cached before
    """
    Returns the 'distribution' attribute of a 'describe application package' SQL query, in lowercase.
    """
    try:
        desc_cursor = execute_query_with_role(
            conn, package_role, f"describe application package {package_name}"
        )
    except ProgrammingError as err:
        generic_sql_error_handler(
            err, package_role
        )  # TODO: This originally did not include package_role

    if desc_cursor.rowcount is None or desc_cursor.rowcount == 0:
        raise SnowflakeSQLExecutionError()
    else:
        for row in desc_cursor:
            if row[0].lower() == "distribution":
                return row[1].lower()
    raise ProgrammingError(
        msg=dedent(
            f"""\
            Could not find the 'distribution' attribute for application package {package_name} in the output of SQL query:
            'describe application package {package_name}'
            """
        )
    )


def verify_project_distribution(  # TODO: Review all (external) usages
    conn: SnowflakeConnection,
    package_name: str,
    package_role: str,
    package_distribution: str,
    expected_distribution: Optional[str] = None,
) -> bool:
    """
    Returns true if the 'distribution' attribute of an existing application package in snowflake
    is the same as the the attribute specified in project definition file.
    """
    actual_distribution = (
        expected_distribution
        if expected_distribution
        else get_app_pkg_distribution_in_snowflake(
            conn, package_name, package_role
        )  # TODO
    )
    project_def_distribution = package_distribution.lower()
    if actual_distribution != project_def_distribution:
        cc.warning(
            dedent(
                f"""\
                Application package {package_name} in your Snowflake account has distribution property {actual_distribution},
                which does not match the value specified in project definition file: {project_def_distribution}.
                """
            )
        )
        return False
    return True


def sync_deploy_root_with_stage(  # TODO: Review all (external) usages
    conn: SnowflakeConnection,
    role: str,
    package_name: str,
    stage_schema: str,
    stage_fqn: str,
    deploy_root: Path,
) -> DiffResult:
    """
    Ensures that the files on our remote stage match the artifacts we have in
    the local filesystem. Returns the DiffResult used to make changes.
    """

    # Does a stage already exist within the application package, or we need to create one?
    # Using "if not exists" should take care of either case.
    cc.step("Checking if stage exists, or creating a new one if none exists.")
    execute_query_with_role(
        conn, role, f"create schema if not exists {package_name}.{stage_schema}"
    )
    execute_query_with_role(
        conn,
        role,
        f"""
            create stage if not exists {stage_fqn}
            encryption = (TYPE = 'SNOWFLAKE_SSE')
            DIRECTORY = (ENABLE = TRUE)""",
    )

    # Perform a diff operation and display results to the user for informational purposes
    cc.step(
        "Performing a diff between the Snowflake stage and your local deploy_root ('%s') directory."
        % deploy_root
    )
    diff: DiffResult = stage_diff(
        deploy_root, stage_fqn
    )  # TODO: THis uses global CLI Context
    cc.message(str(diff))

    # Upload diff-ed files to application package stage
    if diff.has_changes():
        cc.step(
            "Uploading diff-ed files from your local %s directory to the Snowflake stage."
            % deploy_root,
        )
        sync_local_diff_with_stage(
            role=role,
            deploy_root_path=deploy_root,
            diff_result=diff,
            stage_path=stage_fqn,
        )
    return diff


def get_existing_app_info(
    conn: SnowflakeConnection, app_name: str, app_role: str
) -> Optional[dict]:  # TODO: Review all (external) usages
    """
    Check for an existing application object by the same name as in project definition, in account.
    It executes a 'show applications like' query and returns the result as single row, if one exists.
    """
    return show_specific_object_with_role(
        conn,
        app_role,  # TODO: From SqlExecutionMixin
        "applications",
        app_name,
        name_col=NAME_COL,
    )


def get_existing_app_pkg_info(
    conn: SnowflakeConnection, package_name: str, package_role: str
) -> Optional[dict]:  # TODO: Review all (external) usages
    """
    Check for an existing application package by the same name as in project definition, in account.
    It executes a 'show application packages like' query and returns the result as single row, if one exists.
    """
    return show_specific_object_with_role(
        conn, package_role, "application packages", package_name, name_col=NAME_COL
    )


def get_snowsight_url(
    conn: SnowflakeConnection, app_name: str
) -> str:  # TODO: Review all (external) usages
    """Returns the URL that can be used to visit this app via Snowsight."""
    name = unquote_identifier(app_name)
    return make_snowsight_url(conn, f"/#/apps/application/{name}")


def create_app_package(  # TODO: Review all (external) usages
    conn: SnowflakeConnection,
    package_name: str,
    package_role: str,
    package_distribution: str,
) -> None:
    """
    Creates the application package with our up-to-date stage if none exists.
    """

    # 1. Check for existing existing application package
    show_obj_row = get_existing_app_pkg_info(conn, package_name, package_role)

    if show_obj_row:
        # 1. Check for the right owner role
        ensure_correct_owner(row=show_obj_row, role=package_role, obj_name=package_name)

        # 2. Check distribution of the existing application package
        actual_distribution = get_app_pkg_distribution_in_snowflake(
            conn, package_name, package_role
        )  # TODO: Review this
        if not verify_project_distribution(
            conn, package_name, package_role, package_distribution, actual_distribution
        ):
            cc.warning(
                f"Continuing to execute `snow app run` on application package {package_name} with distribution '{actual_distribution}'."
            )

        # 3. If actual_distribution is external, skip comment check
        if actual_distribution == INTERNAL_DISTRIBUTION:
            row_comment = show_obj_row[COMMENT_COL]

            if row_comment not in ALLOWED_SPECIAL_COMMENTS:
                raise ApplicationPackageAlreadyExistsError(package_name)

        return

    # If no application package pre-exists, create an application package, with the specified distribution in the project definition file.
    cc.step(f"Creating new application package {package_name} in account.")
    execute_query_with_role(
        conn,
        package_role,
        dedent(
            f"""\
                create application package {package_name}
                    comment = {SPECIAL_COMMENT}
                    distribution = {package_distribution}
            """
        ),
    )


def _apply_package_scripts(
    conn: SnowflakeConnection,
    package_name: str,  # TODO: Use an object for all these parameters
    package_role: str,
    package_warehouse: str | None,
    project_root: Path,
    package_scripts: List[str],
) -> None:
    """
    Assuming the application package exists and we are using the correct role,
    applies all package scripts in-order to the application package.
    """
    env = jinja2.Environment(
        loader=jinja2.loaders.FileSystemLoader(project_root),
        keep_trailing_newline=True,
        undefined=jinja2.StrictUndefined,
    )

    queued_queries = []
    for relpath in package_scripts:
        try:
            template = env.get_template(relpath)
            result = template.render(dict(package_name=package_name))
            queued_queries.append(result)

        except jinja2.TemplateNotFound as e:
            raise MissingPackageScriptError(e.name)

        except jinja2.TemplateSyntaxError as e:
            raise InvalidPackageScriptError(e.name, e)

        except jinja2.UndefinedError as e:
            raise InvalidPackageScriptError(relpath, e)

    # once we're sure all the templates expanded correctly, execute all of them
    try:
        if package_warehouse:
            execute_query_with_role(
                conn, package_role, f"use warehouse {package_warehouse}"
            )

        for i, queries in enumerate(queued_queries):
            cc.step(f"Applying package script: {package_scripts[i]}")
            execute_queries_with_role(conn, package_role, queries)
    except ProgrammingError as err:
        generic_sql_error_handler(err, role=package_role, warehouse=package_warehouse)


def deploy(
    conn: SnowflakeConnection,
    package_role: str,
    package_name: str,
    stage_schema: str,
    stage_fqn: str,
    deploy_root: Path,
    package_distribution: str,
    package_warehouse: str,
    project_root: Path,
    package_scripts: List[str],  # TODO: Use objects for all these parameters
) -> DiffResult:  # TODO: Review all (external) usages
    """app deploy process"""

    # 1. Create an empty application package, if none exists
    create_app_package(conn, package_name, package_role, package_distribution)

    # 2. now that the application package exists, create shared data
    _apply_package_scripts(
        conn,
        package_name,
        package_role,
        package_warehouse,
        project_root,
        package_scripts,
    )

    # 3. Upload files from deploy root local folder to the above stage
    diff = sync_deploy_root_with_stage(
        conn, package_role, package_name, stage_schema, stage_fqn, deploy_root
    )

    return diff


def ensure_correct_owner(row: dict, role: str, obj_name: str) -> None:
    """
    Check if an object has the right owner role
    """
    actual_owner = row[
        OWNER_COL
    ].upper()  # Because unquote_identifier() always returns uppercase str
    if actual_owner != unquote_identifier(role):
        raise UnexpectedOwnerError(obj_name, role, actual_owner)


def generic_sql_error_handler(
    err: ProgrammingError, role: Optional[str] = None, warehouse: Optional[str] = None
):
    # Potential refactor: If moving away from Python 3.8 and 3.9 to >= 3.10, use match ... case
    if err.errno == 2043 or err.msg.__contains__(ERROR_MESSAGE_2043):
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                '{role}' may not have access to warehouse '{warehouse}'.
                Please grant usage privilege on warehouse to this role.
                """
            ),
            errno=err.errno,
        )
    elif err.errno == 606 or err.msg.__contains__(ERROR_MESSAGE_606):
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                Please provide a warehouse for the active session role in your project definition file, config.toml file, or via command line.
                """
            ),
            errno=err.errno,
        )
    elif err.msg.__contains__("does not exist or not authorized"):
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                Please check the name of the resource you are trying to query or the permissions of the role you are using to run the query.
                """
            )
        )
    raise err
