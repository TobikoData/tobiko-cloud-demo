import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from demo_scripts.load_raw_events import RawEventLoader


@pytest.fixture
def mock_bigquery_client():
    return MagicMock()


@pytest.fixture
def raw_event_loader(mock_bigquery_client):
    with patch(
        "demo_scripts.load_raw_events.bigquery.Client",
        return_value=mock_bigquery_client,
    ):
        return RawEventLoader(MagicMock(), "test-project")


def test_generate_fake_data(raw_event_loader):
    num_rows = 5
    end_date = "2023-04-15"
    data = raw_event_loader.generate_fake_data(num_rows, end_date)

    assert len(data) == num_rows
    for row in data:
        assert set(row.keys()) == {
            "event_id",
            "event_name",
            "event_timestamp",
            "user_id",
        }
        assert isinstance(row["event_timestamp"], datetime)
        assert row["event_timestamp"].strftime("%Y-%m-%d") == end_date


def test_create_table_if_not_exists(raw_event_loader, mock_bigquery_client):
    dataset_name = "test_dataset"
    table_name = "test_table"

    # Test when table doesn't exist
    mock_bigquery_client.get_table.side_effect = Exception("Table not found")
    raw_event_loader.create_table_if_not_exists(dataset_name, table_name)
    mock_bigquery_client.create_table.assert_called_once()

    # Test when table exists
    mock_bigquery_client.reset_mock()
    mock_bigquery_client.get_table.side_effect = None
    raw_event_loader.create_table_if_not_exists(dataset_name, table_name)
    mock_bigquery_client.create_table.assert_not_called()


def test_append_to_bigquery_table(raw_event_loader, mock_bigquery_client):
    with patch.object(
        raw_event_loader, "generate_fake_data"
    ) as mock_generate_data, patch.object(
        raw_event_loader, "create_table_if_not_exists"
    ) as mock_create_table:
        mock_generate_data.return_value = [
            {
                "event_id": "1",
                "event_name": "test",
                "event_timestamp": datetime.now(),
                "user_id": "user1",
            }
        ]

        raw_event_loader.append_to_bigquery_table(
            "test_dataset.test_table", 1, "2023-04-15"
        )

        mock_generate_data.assert_called_once_with(1, "2023-04-15")
        mock_create_table.assert_called_once_with("test_dataset", "test_table")
        mock_bigquery_client.load_table_from_dataframe.assert_called_once()
