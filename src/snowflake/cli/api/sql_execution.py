from __future__ import annotations

import logging
from contextlib import contextmanager
from functools import cached_property
from io import StringIO
from textwrap import dedent
from typing import Iterable, Optional, Tuple

from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import (
    DatabaseNotProvidedError,
    SchemaNotProvidedError,
    SnowflakeSQLExecutionError,
)
from snowflake.cli.api.project.util import (
    identifier_to_show_like_pattern,
    unquote_identifier,
)
from snowflake.cli.api.utils.cursor import find_first_row
from snowflake.cli.api.utils.naming_utils import from_qualified_name
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


class SqlExecutionMixin:
    def __init__(self, connection: SnowflakeConnection):
        self._conn = connection

    @cached_property
    def _log(self):
        return logging.getLogger(__name__)

    def _execute_string(
        self,
        sql_text: str,
        remove_comments: bool = False,
        return_cursors: bool = True,
        cursor_class: SnowflakeCursor = SnowflakeCursor,
        **kwargs,
    ) -> Iterable[SnowflakeCursor]:
        """
        This is a custom implementation of SnowflakeConnection.execute_string that returns generator
        instead of list. In case of executing multiple queries are executed one by one. This mean we can
        access result of previous queries while evaluating next one. For example, we can print the results.
        """
        self._log.debug("Executing %s", sql_text)
        stream = StringIO(sql_text)
        stream_generator = self._conn.execute_stream(
            stream, remove_comments=remove_comments, cursor_class=cursor_class, **kwargs
        )
        return stream_generator if return_cursors else list()

    def _execute_query(self, query: str, **kwargs):
        *_, last_result = self._execute_queries(query, **kwargs)
        return last_result

    def _execute_queries(self, queries: str, **kwargs):
        return list(self._execute_string(dedent(queries), **kwargs))

    def use(self, object_type: ObjectType, name: str):
        return self._execute_query(f"use {object_type.value.sf_name} {name}")

    @contextmanager
    def use_role(self, new_role: str):
        """
        Switches to a different role for a while, then switches back.
        This is a no-op if the requested role is already active.
        """
        role_result = self._execute_query(
            f"select current_role()", cursor_class=DictCursor
        ).fetchone()
        prev_role = role_result["CURRENT_ROLE()"]
        is_different_role = new_role.lower() != prev_role.lower()
        if is_different_role:
            self._log.debug("Assuming different role: %s", new_role)
            self._execute_query(f"use role {new_role}")
        try:
            yield
        finally:
            if is_different_role:
                self._execute_query(f"use role {prev_role}")

    def create_password_secret(
        self, name: str, username: str, password: str
    ) -> SnowflakeCursor:
        return self._execute_query(
            f"""
            create secret {name}
            type = password
            username = '{username}'
            password = '{password}'
            """
        )

    def create_api_integration(
        self, name: str, api_provider: str, allowed_prefix: str, secret: Optional[str]
    ) -> SnowflakeCursor:
        return self._execute_query(
            f"""
            create api integration {name}
            api_provider = {api_provider}
            api_allowed_prefixes = ('{allowed_prefix}')
            allowed_authentication_secrets = ({secret if secret else ''})
            enabled = true
            """
        )

    def _execute_schema_query(self, query: str, name: Optional[str] = None, **kwargs):
        """
        Check that a database and schema are provided before executing the query. Useful for operating on schema level objects.
        """
        self.check_database_and_schema_provided(name)
        return self._execute_query(query, **kwargs)

    def check_database_and_schema_provided(self, name: Optional[str] = None) -> None:
        """
        Checks if a database and schema are provided, either through the connection context or a qualified name.
        """
        if name:
            _, schema, database = from_qualified_name(name)
        else:
            schema, database = None, None
        schema = schema or self._conn.schema
        database = database or self._conn.database
        if not database:
            raise DatabaseNotProvidedError()
        if not schema:
            raise SchemaNotProvidedError()

    def to_fully_qualified_name(
        self, name: str, database: Optional[str] = None, schema: Optional[str] = None
    ):
        current_parts = name.split(".")
        if len(current_parts) == 3:
            # already fully qualified name
            return name.upper()

        if not database:
            if not self._conn.database:
                raise DatabaseNotProvidedError()
            database = self._conn.database

        if len(current_parts) == 2:
            # we assume name is in form of `schema.object`
            return f"{database}.{name}".upper()

        schema = schema or self._conn.schema or "public"
        database = database or self._conn.database
        return f"{database}.{schema}.{name}".upper()

    @staticmethod
    def get_name_from_fully_qualified_name(name):
        """
        Returns name of the object from the fully-qualified name.
        Assumes that [name] is in format [[database.]schema.]name
        """
        return from_qualified_name(name)[0]

    @staticmethod
    def _qualified_name_to_in_clause(name: str) -> Tuple[str, Optional[str]]:
        unqualified_name, schema, database = from_qualified_name(name)
        if database:
            in_clause = f"in schema {database}.{schema}"
        elif schema:
            in_clause = f"in schema {schema}"
        else:
            in_clause = None
        return unqualified_name, in_clause

    class InClauseWithQualifiedNameError(ValueError):
        def __init__(self):
            super().__init__("non-empty 'in_clause' passed with qualified 'name'")

    def show_specific_object(
        self,
        object_type_plural: str,
        name: str,
        name_col: str = "name",
        in_clause: str = "",
        check_schema: bool = False,
    ) -> Optional[dict]:
        """
        Executes a "show <objects> like" query for a particular entity with a
        given (optionally qualified) name. This command is useful when the corresponding
        "describe <object>" query does not provide the information you seek.

        Note that this command is analogous to describe and should only return a single row.
        If the target object type is a schema level object, then check_schema should be set to True
        so that the function will verify that a database and schema are provided, either through
        the connection or a qualified name, before executing the query.
        """

        unqualified_name, name_in_clause = self._qualified_name_to_in_clause(name)
        if in_clause and name_in_clause:
            raise self.InClauseWithQualifiedNameError()
        elif name_in_clause:
            in_clause = name_in_clause
        show_obj_query = f"show {object_type_plural} like {identifier_to_show_like_pattern(unqualified_name)} {in_clause}".strip()

        if check_schema:
            show_obj_cursor = self._execute_schema_query(  # type: ignore
                show_obj_query, name=name, cursor_class=DictCursor
            )
        else:
            show_obj_cursor = self._execute_query(  # type: ignore
                show_obj_query, cursor_class=DictCursor
            )

        if show_obj_cursor.rowcount is None:
            raise SnowflakeSQLExecutionError(show_obj_query)
        elif show_obj_cursor.rowcount > 1:
            raise ProgrammingError(
                f"Received multiple rows from result of SQL statement: {show_obj_query}. Usage of 'show_specific_object' may not be properly scoped."
            )

        show_obj_row = find_first_row(
            show_obj_cursor,
            lambda row: row[name_col] == unquote_identifier(unqualified_name),
        )
        return show_obj_row
