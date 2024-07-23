from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json

# Set credentials
SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SQLMESH_CREDENTIALS"])

# Authenticate with BigQuery using environment variable
credentials = service_account.Credentials.from_service_account_info(
    SERVICE_ACCOUNT_INFO
)
client = bigquery.Client(credentials=credentials, project="sqlmesh-public-demo")

# Define the table
dataset_name = "tcloud_raw_data"
table_name = "raw_events"
table_id = f"{client.project}.{dataset_name}.{table_name}"

# SQL to rename the column
sql = f"""
ALTER TABLE `{table_id}`
RENAME COLUMN event_name TO named_events;
"""

# undo the rename change
# sql = f"""
# ALTER TABLE `{table_id}`
# RENAME COLUMN named_events TO event_name;
# """

# Execute the SQL
job = client.query(sql)
job.result()  # Wait for the job to complete

print(f"Column 'event_name' has been renamed to 'named_events' in table {table_id}")

# Verify the change
table = client.get_table(table_id)
print("\nUpdated Schema:")
for field in table.schema:
    print(f"  {field.name}: {field.field_type}")
