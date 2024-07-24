"""This is a public demo script to generate demo data"""

import pandas as pd
import uuid
from datetime import datetime
import random
from google.cloud import bigquery

# Define the list of possible event names
event_names = ["page_view", "product_view", "ad_view", "video_view", "blog_view"]


class RawEventLoader:
    def __init__(self, credentials, project_id):
        self.client = bigquery.Client(credentials=credentials, project=project_id)

    def generate_fake_data(self, num_rows: int, end_date: str):
        end_date_parsed = datetime.strptime(end_date, "%Y-%m-%d")
        data = []
        for _ in range(num_rows):
            event_id = str(uuid.uuid4())
            event_name = random.choice(event_names)
            event_timestamp = end_date_parsed
            user_id = str(uuid.uuid4())
            row = {
                "event_id": event_id,
                "event_name": event_name,
                "event_timestamp": event_timestamp,
                "user_id": user_id,
            }
            data.append(row)
        return data

    def create_table_if_not_exists(self, dataset_name: str, table_name: str):
        dataset_ref = self.client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        try:
            self.client.get_table(table_ref)
        except:
            schema = [
                bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("event_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            print(f"Created table {table.table_id}")

    def append_to_bigquery_table(self, table_name: str, num_rows: int, end_date: str):
        dataset_name, table_name = table_name.split(".")

        fake_data = self.generate_fake_data(num_rows, end_date)
        df = pd.DataFrame(fake_data)

        self.create_table_if_not_exists(dataset_name, table_name)

        job = self.client.load_table_from_dataframe(df, f"{dataset_name}.{table_name}")
        job.result()

        print(
            f"{num_rows} rows of raw events demo data with date [{end_date}] appended to {dataset_name}.{table_name}"
        )
