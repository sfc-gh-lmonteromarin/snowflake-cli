from __future__ import annotations

import logging
import re
from contextlib import nullcontext
from pathlib import Path
from typing import List, Optional, Union

from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.utils.path_utils import path_resolver
from snowflake.connector import DictCursor
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


UNQUOTED_FILE_URI_REGEX = r"[\w/*?\-.=&{}$#[\]\"\\!@%^+:]+"


class StageManager(SqlExecutionMixin):
    @staticmethod
    def get_standard_stage_prefix(name: str) -> str:
        # Handle embedded stages
        if name.startswith("snow://") or name.startswith("@"):
            return name

        return f"@{name}"

    @staticmethod
    def get_standard_stage_directory_path(path):
        if not path.endswith("/"):
            path += "/"
        return StageManager.get_standard_stage_prefix(path)

    @staticmethod
    def get_stage_name_from_path(path: str):
        """
        Returns stage name from potential path on stage. For example
        db.schema.stage/foo/bar  -> db.schema.stage
        """
        return Path(path).parts[0]

    @staticmethod
    def quote_stage_name(name: str) -> str:
        if name.startswith("'") and name.endswith("'"):
            return name  # already quoted

        standard_name = StageManager.get_standard_stage_prefix(name)
        if standard_name.startswith("@") and not re.fullmatch(
            r"@([\w./$])+", standard_name
        ):
            return to_string_literal(standard_name)

        return standard_name

    def _to_uri(self, local_path: str):
        uri = f"file://{local_path}"
        if re.fullmatch(UNQUOTED_FILE_URI_REGEX, uri):
            return uri
        return to_string_literal(uri)

    def list_files(self, stage_name: str, pattern: str | None = None) -> DictCursor:
        stage_name = self.get_standard_stage_prefix(stage_name)
        query = f"ls {self.quote_stage_name(stage_name)}"
        if pattern is not None:
            query += f" pattern = '{pattern}'"
        return self._execute_query(query, cursor_class=DictCursor)

    @staticmethod
    def _assure_is_existing_directory(path: Path) -> None:
        spath = SecurePath(path)
        if not spath.exists():
            spath.mkdir(parents=True)
        spath.assert_is_directory()

    def get(
        self, stage_path: str, dest_path: Path, parallel: int = 4
    ) -> SnowflakeCursor:
        stage_path = self.get_standard_stage_prefix(stage_path)
        self._assure_is_existing_directory(dest_path)
        dest_directory = f"{dest_path}/"
        return self._execute_query(
            f"get {self.quote_stage_name(stage_path)} {self._to_uri(dest_directory)} parallel={parallel}"
        )

    def get_recursive(
        self, stage_path: str, dest_path: Path, parallel: int = 4
    ) -> List[SnowflakeCursor]:
        stage_path_only = stage_path
        if stage_path_only.startswith("snow://"):
            stage_path_only = stage_path_only[7:]
        stage_parts_length = len(Path(stage_path_only).parts)

        results = []
        for file in self.iter_stage(stage_path):
            dest_directory = dest_path / "/".join(
                Path(file).parts[stage_parts_length:-1]
            )
            self._assure_is_existing_directory(Path(dest_directory))

            stage_path_with_prefix = self.get_standard_stage_prefix(file)

            result = self._execute_query(
                f"get {self.quote_stage_name(stage_path_with_prefix)} {self._to_uri(f'{dest_directory}/')} parallel={parallel}"
            )
            results.append(result)

        return results

    def put(
        self,
        local_path: Union[str, Path],
        stage_path: str,
        parallel: int = 4,
        overwrite: bool = False,
        role: Optional[str] = None,
    ) -> SnowflakeCursor:
        """
        This method will take a file path from the user's system and put it into a Snowflake stage,
        which includes its fully qualified name as well as the path within the stage.
        If provided with a role, then temporarily use this role to perform the operation above,
        and switch back to the original role for the next commands to run.
        """
        with self.use_role(role) if role else nullcontext():
            stage_path = self.get_standard_stage_prefix(stage_path)
            local_resolved_path = path_resolver(str(local_path))
            log.info("Uploading %s to @%s", local_resolved_path, stage_path)
            cursor = self._execute_query(
                f"put {self._to_uri(local_resolved_path)} {self.quote_stage_name(stage_path)} "
                f"auto_compress=false parallel={parallel} overwrite={overwrite}"
            )
        return cursor

    def copy_files(self, source_path: str, destination_path: str) -> SnowflakeCursor:
        source = self.get_standard_stage_prefix(source_path)
        destination = self.get_standard_stage_directory_path(destination_path)
        log.info("Copying files from %s to %s", source, destination)
        query = f"copy files into {destination} from {source}"
        return self._execute_query(query)

    def remove(
        self, stage_name: str, path: str, role: Optional[str] = None
    ) -> SnowflakeCursor:
        """
        This method will take a file path that exists on a Snowflake stage,
        and remove it from the stage.
        If provided with a role, then temporarily use this role to perform the operation above,
        and switch back to the original role for the next commands to run.
        """
        with self.use_role(role) if role else nullcontext():
            stage_name = self.get_standard_stage_prefix(stage_name)
            path = path if path.startswith("/") else "/" + path
            quoted_stage_name = self.quote_stage_name(f"{stage_name}{path}")
            return self._execute_query(f"remove {quoted_stage_name}")

    def create(self, stage_name: str, comment: Optional[str] = None) -> SnowflakeCursor:
        query = f"create stage if not exists {stage_name}"
        if comment:
            query += f" comment='{comment}'"
        return self._execute_query(query)

    def iter_stage(self, stage_path: str):
        for file in self.list_files(stage_path).fetchall():
            yield file["name"]
