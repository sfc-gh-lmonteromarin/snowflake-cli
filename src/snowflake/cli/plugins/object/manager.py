from __future__ import annotations

from typing import Optional, Tuple, Union

from click import ClickException
from snowflake.cli.api.constants import OBJECT_TO_NAMES, ObjectNames
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor


def _get_object_names(object_type: str) -> ObjectNames:
    object_type = object_type.lower()
    if object_type.lower() not in OBJECT_TO_NAMES:
        raise ClickException(f"Object of type {object_type} is not supported.")
    return OBJECT_TO_NAMES[object_type]


class ObjectManager(SqlExecutionMixin):
    def show(
        self,
        *,
        object_type: str,
        like: Optional[str] = None,
        scope: Union[Tuple[str, str], Tuple[None, None]] = (None, None),
        **kwargs,
    ) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_plural_name
        query = f"show {object_name}"
        if like:
            query += f" like '{like}'"
        if scope[0] is not None:
            query += f" in {scope[0].replace('-', ' ')} {scope[1]}"
        return self._execute_query(query, **kwargs)

    def drop(self, *, object_type, name: str) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"drop {object_name} {name}")

    def describe(self, *, object_type: str, name: str):
        # Image repository is the only supported object that does not have a DESCRIBE command.
        if object_type == "image-repository":
            raise ClickException(
                f"Describe is currently not supported for object of type image-repository"
            )
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"describe {object_name} {name}")

    def object_exists(self, *, object_type: str, name: str):
        try:
            self.describe(object_type=object_type, name=name)
            return True
        except ProgrammingError:
            return False
