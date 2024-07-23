from google.cloud import bigquery
from google.oauth2 import service_account

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

    # SQL to rename the column
    sql = f"ALTER TABLE `{table_id}` RENAME COLUMN {column_to_rename} TO {new_column_name};"

    # Execute the SQL
    job = client.query(sql)
    job.result()  # Wait for the job to complete

    print(
        f"Column '{column_to_rename}' has been renamed to '{new_column_name}' in table {table_id}"
    )

    # Verify the change
    table = client.get_table(table_id)
    updated_schema = "\n".join(
        f"  {field.name}: {field.field_type}" for field in table.schema
    )
    print("\nUpdated Schema:")
    print(updated_schema)