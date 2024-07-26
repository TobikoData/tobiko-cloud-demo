from google.cloud import bigquery
from google.oauth2 import service_account
import typer


def rename_column_util(
    credentials,
    project_name: str,
    dataset_name: str,
    table_name: str,
    column_to_rename: str,
    new_column_name: str,
):
    # Use the provided credentials
    client = bigquery.Client(credentials=credentials, project=project_name)

    # Define the table
    table_id = f"{client.project}.{dataset_name}.{table_name}"

    # Get the original schema
    table = client.get_table(table_id)
    original_schema = "\n".join(
        f"  {field.name}: {field.field_type}" for field in table.schema
    )
    typer.echo("\nOriginal Schema:")
    typer.echo(original_schema)

    # SQL to rename the column
    sql = f"ALTER TABLE `{table_id}` RENAME COLUMN {column_to_rename} TO {new_column_name};"

    try:
        # Execute the SQL
        job = client.query(sql)
        job.result()  # Wait for the job to complete

        typer.echo(
            f"\nColumn '{typer.style(column_to_rename, fg=typer.colors.RED)}' "
            f"has been renamed to '{typer.style(new_column_name, fg=typer.colors.GREEN)}' "
            f"in table {table_id}"
        )

        # Verify the change
        table = client.get_table(table_id)
        updated_schema = "\n".join(
            f"  {field.name}: {field.field_type}" for field in table.schema
        )
        typer.echo("\nUpdated Schema:")
        typer.echo(updated_schema)
    except Exception as e:
        if "Column already exists" in str(e):
            print(f"\nError: {e}")
        else:
            raise
