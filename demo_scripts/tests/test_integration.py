import pytest
from unittest.mock import patch, MagicMock
from demo_scripts.main import app
from typer.testing import CliRunner


@pytest.fixture
def mock_bigquery_client():
    return MagicMock()


@pytest.fixture
def mock_credentials():
    return MagicMock()


@pytest.fixture
def runner():
    return CliRunner()


@patch("demo_scripts.main.get_service_account_info")
@patch("google.oauth2.service_account.Credentials.from_service_account_info")
@patch("google.cloud.bigquery.Client")
def test_integration_append_and_rename(
    mock_bq_client, mock_credentials, mock_get_sa_info, runner, mock_bigquery_client
):
    mock_get_sa_info.return_value = {}
    mock_credentials.return_value = MagicMock()
    mock_bq_client.return_value = mock_bigquery_client

    # Append raw data
    result = runner.invoke(
        app,
        [
            "append-rawdata",
            "--num-rows",
            "10",
            "--table-name",
            "test_dataset.test_table",
        ],
    )
    assert result.exit_code == 0
    mock_bigquery_client.load_table_from_dataframe.assert_called_once()

    # Rename column
    result = runner.invoke(
        app, ["rename-column", "--old", "event_name", "--new", "renamed_event"]
    )
    assert result.exit_code == 0
    mock_bigquery_client.query.assert_called_once()

    # Check if the table was retrieved twice (before and after renaming)
    assert mock_bigquery_client.get_table.call_count == 2


@patch("demo_scripts.main.get_service_account_info")
@patch("google.oauth2.service_account.Credentials.from_service_account_info")
@patch("google.cloud.bigquery.Client")
def test_integration_error_handling(
    mock_bq_client, mock_credentials, mock_get_sa_info, runner, mock_bigquery_client
):
    mock_get_sa_info.return_value = {}
    mock_credentials.return_value = MagicMock()
    mock_bq_client.return_value = mock_bigquery_client

    # Simulate an error when appending data
    mock_bigquery_client.load_table_from_dataframe.side_effect = Exception(
        "Failed to append data"
    )

    result = runner.invoke(
        app,
        [
            "append-rawdata",
            "--num-rows",
            "10",
            "--table-name",
            "test_dataset.test_table",
        ],
    )
    assert result.exit_code != 0
    assert "Failed to append data" in result.output

    # Simulate an error when renaming column
    mock_bigquery_client.query.side_effect = Exception("Failed to rename column")

    result = runner.invoke(
        app, ["rename-column", "--old", "event_name", "--new", "renamed_event"]
    )
    assert result.exit_code != 0
    assert "Failed to rename column" in result.output
