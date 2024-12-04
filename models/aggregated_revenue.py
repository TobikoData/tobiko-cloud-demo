import typing as t
from datetime import datetime

import pandas as pd
from sqlmesh import ExecutionContext, model

@model(
    "tcloud_demo.aggregated_revenue",
    owner="afzal",
    cron="@daily",
    columns={
        "customer_id": "int",
        "total_order_amount": "float",
        "order_count": "int",
        "aov": "float",
        "lifetime": "float",
        "clv": "float",

    },
    column_descriptions={
        "customer_id": "Unique identifier for each customer",
        "total_order_amount": "The total amount spent by the customer across all orders",
        "order_count": "The total number of orders placed by the customer",
        "aov": "The average amount spent per order by the customer",
        "lifetime": "The customer relationship duration in years, calculated from the first to last order date",
        "clv": "The projected revenue a customer will bring over their lifetime with the business",
    }
)

def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # Fetch data from the orders model, automatically captures the model's dependencies
    table = context.table("tcloud_demo.orders")
    df = context.fetchdf(f"SELECT * FROM {table}")


    # Step 1: Convert 'order_date' to datetime format
    df['order_date'] = pd.to_datetime(df['order_date'])

    # Step 2: Calculate total order amount and number of orders for each customer
    customer_totals = df.groupby('customer_id')['amount'].sum().reset_index()
    customer_totals.rename(columns={'amount': 'total_order_amount'}, inplace=True)
    customer_order_count = df.groupby('customer_id').size().reset_index(name='order_count')

    # Merge total order amount and order count
    customer_summary = customer_totals.merge(customer_order_count, on='customer_id')

    # Calculate Average Order Value (AOV)
    customer_summary['aov'] = customer_summary['total_order_amount'] / customer_summary['order_count']

    # Step 3: Calculate dynamic lifetime (in years) for each customer
    customer_lifetime = df.groupby('customer_id')['order_date'].agg(['min', 'max']).reset_index()
    customer_lifetime['lifetime'] = (customer_lifetime['max'] - customer_lifetime['min']).dt.days / 365.0

    # Merge Lifetime with customer summary
    customer_summary = customer_summary.merge(customer_lifetime[['customer_id', 'lifetime']], on='customer_id')

    # Step 4: Calculate CLV using dynamic Lifetime
    customer_summary['clv'] = customer_summary['aov'] * customer_summary['order_count'] * customer_summary['lifetime']

    
    # Select only the columns specified in the model definition
    result = customer_summary[['customer_id','total_order_amount','order_count','aov','lifetime','clv']]

    return result