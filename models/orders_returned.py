
import typing as t
from datetime import datetime

import pandas as pd
from sqlmesh import ExecutionContext, model

@model(
    "tcloud_demo.orders_returned",
    owner="sung",
    cron="@daily",
    columns={
        "order_id": "int",
        "customer_id": "int",
        "order_date": "date",
        "status": "text",
    },
    column_descriptions={
        "order_id": "Unique ID",
        "customer_id": "Customer ID",
        "order_date": "Order Date",
        "status": "Order Status",
    },
    audits=[
        ("not_null", {"columns": ["order_id", "customer_id"]}),
    ],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # Fetch data from the stg_orders model, automatically captures the model's dependencies
    table = context.table("tcloud_demo.stg_orders")
    df = context.fetchdf(f"SELECT * FROM {table}")

    # Filter only where status equals "returned"
    df_returned = df[df['status'] == 'returned']
    
    # Select only the columns specified in the model definition
    result = df_returned[['order_id', 'customer_id', 'order_date', 'status']]
    
    return result