from pathlib import Path
from typing import List, Optional
from unittest import mock
from unittest.mock import PropertyMock

import pytest
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.project.definition import (
    generate_local_override_yml,
    load_project_definition,
)
from snowflake.cli.api.project.errors import SchemaValidationError


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_napp_project_1(project_definition_files):
    project = load_project_definition(project_definition_files)
    assert project.native_app.name == "myapp"
    assert project.native_app.deploy_root == "output/deploy/"
    assert project.native_app.package.role == "accountadmin"
    assert project.native_app.application.name == "myapp_polly"
    assert project.native_app.application.role == "myapp_consumer"
    assert project.native_app.application.debug == True


@pytest.mark.parametrize("project_definition_files", ["minimal"], indirect=True)
def test_na_minimal_project(project_definition_files: List[Path]):
    project = load_project_definition(project_definition_files)
    assert project.native_app.name == "minimal"
    assert project.native_app.artifacts == ["setup.sql", "README.md"]

    from os import getenv as original_getenv

    def mock_getenv(key: str, default: Optional[str] = None) -> Optional[str]:
        if key.lower() == "user":
            return "jsmith"
        return original_getenv(key, default)

    with mock.patch(
        "snowflake.cli.api.cli_global_context._CliGlobalContextAccess.connection",
        new_callable=PropertyMock,
    ) as connection:
        connection.return_value.role = "resolved_role"
        connection.return_value.warehouse = "resolved_warehouse"
        with mock.patch("os.getenv", side_effect=mock_getenv):
            # TODO: probably a better way of going about this is to not generate
            # a definition structure for these values but directly return defaults
            # in "getter" functions (higher-level data structures).
            local = generate_local_override_yml(cli_context.connection, project)
            assert local.native_app.application.name == "minimal_jsmith"
            assert local.native_app.application.role == "resolved_role"
            assert local.native_app.application.warehouse == "resolved_warehouse"
            assert local.native_app.application.debug == True
            assert local.native_app.package.name == "minimal_pkg_jsmith"
            assert local.native_app.package.role == "resolved_role"


@pytest.mark.parametrize("project_definition_files", ["underspecified"], indirect=True)
def test_underspecified_project(project_definition_files):
    with pytest.raises(SchemaValidationError) as exc_info:
        load_project_definition(project_definition_files)

    assert "NativeApp schema" in str(exc_info)
    assert "Your project definition is missing following fields: ('artifacts',)" in str(
        exc_info.value
    )


@pytest.mark.parametrize(
    "project_definition_files", ["no_definition_version"], indirect=True
)
def test_fails_without_definition_version(project_definition_files):
    with pytest.raises(SchemaValidationError) as exc_info:
        load_project_definition(project_definition_files)

    assert "ProjectDefinition" in str(exc_info)
    assert (
        "Your project definition is missing following fields: ('definition_version',)"
        in str(exc_info.value)
    )


@pytest.mark.parametrize("project_definition_files", ["unknown_fields"], indirect=True)
def test_does_not_accept_unknown_fields(project_definition_files):
    with pytest.raises(SchemaValidationError) as exc_info:
        project = load_project_definition(project_definition_files)

    assert "NativeApp schema" in str(exc_info)
    assert (
        "You provided field '('unknown_fields_accepted',)' with value 'true' that is not present in the schema"
        in str(exc_info)
    )


@pytest.mark.parametrize(
    "project_definition_files",
    [
        "integration",
        "integration_external",
        "minimal",
        "napp_project_1",
        "napp_project_with_pkg_warehouse",
        "snowpark_function_external_access",
        "snowpark_function_fully_qualified_name",
        "snowpark_function_secrets_without_external_access",
        "snowpark_functions",
        "snowpark_procedure_external_access",
        "snowpark_procedure_fully_qualified_name",
        "snowpark_procedure_secrets_without_external_access",
        "snowpark_procedures",
        "snowpark_procedures_coverage",
        "streamlit_full_definition",
    ],
    indirect=True,
)
def test_fields_are_parsed_correctly(project_definition_files, snapshot):
    result = load_project_definition(project_definition_files).model_dump()
    assert result == snapshot
