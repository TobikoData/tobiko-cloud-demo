import typer
from google.oauth2 import service_account
from rename_column_util import rename_column_util
from load_raw_events import RawEventLoader
from datetime import datetime
from config import get_service_account_info

app = typer.Typer()

@app.command()
def rename_column(
    project_name: str = typer.Option("sqlmesh-public-demo", help="The Google Cloud project name."),
    dataset_name: str = typer.Option("tcloud_raw_data", help="The BigQuery dataset name."),
    table_name: str = typer.Option("raw_events", help="The BigQuery table name."),
    old: str = typer.Option("named_events", help="The name of the column to be renamed."),
    new: str = typer.Option("event_name", help="The new name for the column."),
):
    """
    Rename a column in a BigQuery table to create an error OR fix for a raw table
    """
    # Get the service account info securely
    service_account_info = get_service_account_info()
    
    # Create credentials object
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    
    # Call the rename_column_util with credentials
    rename_column_util(
        credentials=credentials,
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=table_name,
        column_to_rename=old,
        new_column_name=new,
    )


@app.command()
def append_rawdata(
    table_name: str = typer.Option("tcloud_raw_data.raw_events", help="The fully qualified BigQuery table name (dataset.table)."),
    num_rows: int = typer.Option(20, help="The number of rows to append."),
    end_date: str = typer.Option(datetime.today().strftime('%Y-%m-%d'), help="End date in YYYY-MM-DD format. Defaults to today's date."),
    project_id: str = typer.Option("sqlmesh-public-demo", help="The Google Cloud project ID."),
):
    """
    Append raw data to a BigQuery table intended to impact the incremental_events.sql model
    """
    service_account_info = get_service_account_info()
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    
    loader = RawEventLoader(credentials, project_id)
    loader.append_to_bigquery_table(table_name, num_rows, end_date)


if __name__ == "__main__":
    app()
