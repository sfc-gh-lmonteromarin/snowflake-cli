from pathlib import Path
from unittest import mock

SNOWCLI_CONFIG = "snowcli.cli.stage.config"
APP_CONFIG = "snowcli.cli.stage.AppConfig"


@mock.patch(APP_CONFIG)
@mock.patch(SNOWCLI_CONFIG)
def test_default_path_in_get_command(mock_config, mock_app_config, runner):
    mock_config.isAuth.return_value = True
    mock_app_config.return_value.config.get.return_value = {
        "database": "some_database",
        "schema": "some_schema",
        "role": "some_role",
        "warehouse": "some_warehouse",
    }

    result = runner.invoke(["stage", "get", "some_name"])

    assert result.exit_code == 0
    mock_config.connectToSnowflake.assert_called_once()
    mock_config.snowflake_connection.getStage.assert_called_once_with(
        database="some_database",
        schema="some_schema",
        role="some_role",
        warehouse="some_warehouse",
        name="some_name",
        path=str(Path(".").absolute()),
    )