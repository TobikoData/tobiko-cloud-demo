import pytest
from typer.testing import CliRunner
from demo_scripts.main import app
from unittest.mock import patch, MagicMock, ANY

runner = CliRunner()


@pytest.fixture
def mock_credentials():
    return MagicMock()


@pytest.fixture
def mock_rename_column_util():
    with patch("demo_scripts.main.rename_column_util") as mock:
        yield mock


@pytest.fixture
def mock_raw_event_loader():
    with patch("demo_scripts.main.RawEventLoader") as mock:
        yield mock


def test_rename_column(mock_credentials, mock_rename_column_util):
    with patch("demo_scripts.main.get_service_account_info", return_value={}), patch(
        "demo_scripts.main.service_account.Credentials.from_service_account_info",
        return_value=mock_credentials,
    ):
        result = runner.invoke(
            app, ["rename-column", "--old", "old_name", "--new", "new_name"]
        )
        assert result.exit_code == 0
        mock_rename_column_util.assert_called_once()


def test_append_rawdata(mock_credentials, mock_raw_event_loader):
    with patch("demo_scripts.main.get_service_account_info", return_value={}), patch(
        "demo_scripts.main.service_account.Credentials.from_service_account_info",
        return_value=mock_credentials,
    ):
        result = runner.invoke(app, ["append-rawdata", "--num-rows", "10"])
        assert result.exit_code == 0
        mock_raw_event_loader.assert_called_once()
        mock_raw_event_loader.return_value.append_to_bigquery_table.assert_called_once()

def test_append_rawdata_with_custom_table(mock_credentials, mock_raw_event_loader):
    with patch("demo_scripts.main.get_service_account_info", return_value={}), patch(
        "demo_scripts.main.service_account.Credentials.from_service_account_info",
        return_value=mock_credentials,
    ):
        result = runner.invoke(app, ["append-rawdata", "--num-rows", "10", "--table-name", "custom_dataset.custom_table"])
        assert result.exit_code == 0
        mock_raw_event_loader.assert_called_once()
        mock_raw_event_loader.return_value.append_to_bigquery_table.assert_called_once_with("custom_dataset.custom_table", 10, ANY)

def test_append_rawdata_invalid_num_rows(mock_credentials, mock_raw_event_loader):
    result = runner.invoke(app, ["append-rawdata", "--num-rows", "-5"])
    assert result.exit_code != 0

def test_cli_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0

def test_invalid_command():
    result = runner.invoke(app, ["invalid-command"])
    assert result.exit_code != 0
