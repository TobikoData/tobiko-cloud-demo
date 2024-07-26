import pytest
from unittest.mock import MagicMock, patch
from demo_scripts.rename_column_util import rename_column_util


@pytest.fixture
def mock_bigquery_client():
    return MagicMock()


@pytest.fixture
def mock_table():
    table = MagicMock()
    table.schema = [
        MagicMock(name="old_column", field_type="STRING"),
        MagicMock(name="other_column", field_type="INTEGER"),
    ]
    return table


def test_rename_column_util(mock_bigquery_client, mock_table):
    with patch(
        "demo_scripts.rename_column_util.bigquery.Client",
        return_value=mock_bigquery_client,
    ):
        mock_bigquery_client.get_table.return_value = mock_table

        rename_column_util(
            MagicMock(),
            "test-project",
            "test_dataset",
            "test_table",
            "old_column",
            "new_column",
        )

        # Check if the query was executed
        mock_bigquery_client.query.assert_called_once()

        # Check if the table was retrieved twice (before and after renaming)
        assert mock_bigquery_client.get_table.call_count == 2


def test_rename_column_util_column_exists(mock_bigquery_client, mock_table):
    with patch(
        "demo_scripts.rename_column_util.bigquery.Client",
        return_value=mock_bigquery_client,
    ):
        mock_bigquery_client.get_table.return_value = mock_table
        mock_bigquery_client.query.side_effect = Exception("Column already exists")

        # The function should not raise an exception when the column already exists
        rename_column_util(
            MagicMock(),
            "test-project",
            "test_dataset",
            "test_table",
            "old_column",
            "new_column",
        )


def test_rename_column_util_other_exception(mock_bigquery_client, mock_table):
    with patch(
        "demo_scripts.rename_column_util.bigquery.Client",
        return_value=mock_bigquery_client,
    ):
        mock_bigquery_client.get_table.return_value = mock_table
        mock_bigquery_client.query.side_effect = Exception("Some other error")

        # The function should raise an exception for other errors
        with pytest.raises(Exception):
            rename_column_util(
                MagicMock(),
                "test-project",
                "test_dataset",
                "test_table",
                "old_column",
                "new_column",
            )
