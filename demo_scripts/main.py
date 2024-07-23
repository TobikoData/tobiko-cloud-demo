import typer
from google.oauth2 import service_account
from rename_column_util import rename_column_util
from load_raw_events import RawEventLoader
from datetime import datetime
from config import get_service_account_info

app = typer.Typer()

@app.command()
def hello(name: str):
    print(f"Hello {name}")


@app.command()
def goodbye(name: str, formal: bool = False):
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")


@app.command()
def rename_column(
    project_name: str = "sqlmesh-public-demo",
    dataset_name: str = "tcloud_raw_data",
    table_name: str = "raw_events",
    column_to_rename: str = "named_events",
    new_column_name: str = "event_name",
):
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
        column_to_rename=column_to_rename,
        new_column_name=new_column_name,
    )


@app.command()
def append_rawdata(
    table_name: str = "tcloud_raw_data.raw_events",
    num_rows: int = 20,
    end_date: str = typer.Option(datetime.today().strftime('%Y-%m-%d'), help="End date in YYYY-MM-DD format"),
    project_id: str = "sqlmesh-public-demo",
):
    service_account_info = get_service_account_info()
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    
    loader = RawEventLoader(credentials, project_id)
    loader.append_to_bigquery_table(table_name, num_rows, end_date)


if __name__ == "__main__":
    app()