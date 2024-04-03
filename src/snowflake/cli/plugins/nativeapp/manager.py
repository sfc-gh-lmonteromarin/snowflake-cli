from __future__ import annotations

from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from textwrap import dedent
from typing import List, Optional

from snowflake.cli.api.project.definition import (
    default_app_package,
    default_application,
    default_role,
)
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.util import (
    extract_schema,
    to_identifier,
    unquote_identifier,
)
from snowflake.cli.plugins.nativeapp.artifacts import (
    ArtifactMapping,
    build_bundle,
    translate_artifact,
)
from snowflake.cli.plugins.nativeapp.constants import (
    ERROR_MESSAGE_606,
    ERROR_MESSAGE_2043,
    OWNER_COL,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    UnexpectedOwnerError,
)
from snowflake.cli.plugins.object.stage.diff import (
    DiffResult,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.connection import SnowflakeConnection

from src.snowflake.cli.plugins.nativeapp.management import (
    create_app_package,
    deploy,
    get_app_pkg_distribution_in_snowflake,
    get_existing_app_info,
    get_existing_app_pkg_info,
    get_snowsight_url,
    sync_deploy_root_with_stage,
    verify_project_distribution,
)


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


def ensure_correct_owner(row: dict, role: str, obj_name: str) -> None:
    """
    Check if an object has the right owner role
    """
    actual_owner = row[
        OWNER_COL
    ].upper()  # Because unquote_identifier() always returns uppercase str
    if actual_owner != unquote_identifier(role):
        raise UnexpectedOwnerError(obj_name, role, actual_owner)


class NativeAppCommandProcessor(ABC):
    @abstractmethod
    def process(self, *args, **kwargs):
        pass


class NativeAppManager:
    """
    Base class with frequently used functionality already implemented and ready to be used by related subclasses.
    """

    def __init__(
        self,
        conn: SnowflakeConnection,
        project_definition: NativeApp,
        project_root: Path,
    ):
        super().__init__()
        self._conn = conn
        self._project_root = project_root
        self._project_definition = project_definition

    @property
    def project_root(self) -> Path:
        return self._project_root

    @property
    def definition(self) -> NativeApp:
        return self._project_definition

    @cached_property
    def artifacts(self) -> List[ArtifactMapping]:
        return [translate_artifact(item) for item in self.definition.artifacts]

    @cached_property
    def deploy_root(self) -> Path:
        return Path(self.project_root, self.definition.deploy_root)

    @cached_property
    def package_scripts(self) -> List[str]:
        """
        Relative paths to package scripts from the project root.
        """
        if self.definition.package and self.definition.package.scripts:
            return self.definition.package.scripts
        else:
            return []

    @cached_property
    def stage_fqn(self) -> str:
        return f"{self.package_name}.{self.definition.source_stage}"

    @cached_property
    def stage_schema(self) -> Optional[str]:
        return extract_schema(self.stage_fqn)

    @cached_property
    def package_warehouse(self) -> Optional[str]:
        if self.definition.package and self.definition.package.warehouse:
            return self.definition.package.warehouse
        else:
            return self._conn.warehouse

    @cached_property
    def application_warehouse(self) -> Optional[str]:
        if self.definition.application and self.definition.application.warehouse:
            return self.definition.application.warehouse
        else:
            return self._conn.warehouse

    @cached_property
    def project_identifier(self) -> str:
        # name is expected to be a valid Snowflake identifier, but PyYAML
        # will sometimes strip out double quotes so we try to get them back here.
        return to_identifier(self.definition.name)

    @cached_property
    def package_name(self) -> str:
        if self.definition.package and self.definition.package.name:
            return to_identifier(self.definition.package.name)
        else:
            return to_identifier(default_app_package(self.project_identifier))

    @cached_property
    def package_role(self) -> str:
        if self.definition.package and self.definition.package.role:
            return self.definition.package.role
        else:
            return default_role()

    @cached_property
    def package_distribution(self) -> str:
        if self.definition.package and self.definition.package.distribution:
            return self.definition.package.distribution.lower()
        else:
            return "internal"

    @cached_property
    def app_name(self) -> str:
        if self.definition.application and self.definition.application.name:
            return to_identifier(self.definition.application.name)
        else:
            return to_identifier(default_application(self.project_identifier))

    @cached_property
    def app_role(self) -> str:
        if self.definition.application and self.definition.application.role:
            return self.definition.application.role
        else:
            return default_role()

    @cached_property
    def debug_mode(self) -> bool:
        if self.definition.application:
            return self.definition.application.debug
        else:
            return True

    @cached_property
    def get_app_pkg_distribution_in_snowflake(self) -> str:
        return get_app_pkg_distribution_in_snowflake(
            self._conn, self.package_name, self.package_role
        )

    def verify_project_distribution(
        self, expected_distribution: Optional[str] = None
    ) -> bool:
        return verify_project_distribution(
            self._conn,
            self.package_name,
            self.package_role,
            self.package_distribution,
            expected_distribution,
        )

    def build_bundle(self) -> None:
        """
        Populates the local deploy root from artifact sources.
        """
        build_bundle(self.project_root, self.deploy_root, self.artifacts)

    def sync_deploy_root_with_stage(self, role: str) -> DiffResult:
        if self.stage_schema is None:
            raise ValueError("Change this")  # TODO
        return sync_deploy_root_with_stage(
            self._conn,
            role,
            self.package_name,
            self.stage_schema,
            self.stage_fqn,
            self.deploy_root,
        )

    def get_existing_app_info(self) -> Optional[dict]:
        return get_existing_app_info(self._conn, self.app_name, self.app_role)

    def get_existing_app_pkg_info(self) -> Optional[dict]:
        """
        Check for an existing application package by the same name as in project definition, in account.
        It executes a 'show application packages like' query and returns the result as single row, if one exists.
        """
        return get_existing_app_pkg_info(
            self._conn, self.package_name, self.package_role
        )

    def get_snowsight_url(self) -> str:
        """Returns the URL that can be used to visit this app via Snowsight."""
        return get_snowsight_url(self._conn, self.app_name)

    def create_app_package(self) -> None:
        """
        Creates the application package with our up-to-date stage if none exists.
        """
        return create_app_package(
            self._conn, self.package_name, self.package_role, self.package_distribution
        )

    def deploy(self) -> DiffResult:
        """app deploy process"""
        if self.stage_schema is None or self.package_warehouse is None:
            raise ValueError("Change this")  # TODO
        return deploy(
            self._conn,
            self.package_role,
            self.package_name,
            self.stage_schema,
            self.stage_fqn,
            self.deploy_root,
            self.package_distribution,
            self.package_warehouse,
            self.project_root,
            self.package_scripts,
        )
