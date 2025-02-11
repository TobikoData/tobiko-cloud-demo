import subprocess
import pandas as pd
import re
import typing as t
from datetime import datetime
from sqlmesh import ExecutionContext, model


def parse_output_to_df(table, output):
    """Extract std output into dataframe"""

    # Extract schema match (if present)
    schema_match = "Schemas match" in output

    # Extract table name (assuming format always contains 'Schema Diff Between ... and ...')
    table_match = re.search(r"Schema Diff Between .*?\.([\w\d_]+)' and .*?\.([\w\d_]+)'", output)
    table_name = table_match.group(1) if table_match else None

    # Extract row count values and percentages
    row_counts = {
        "full_match": None,
        "full_match_pct": None,
        "partial_match": None,
        "partial_match_pct": None,
        "only_match": None,
        "only_match_pct": None
    }

    full_match = re.search(r"FULL MATCH:\s*([\d,]+) rows \(([\d.]+)%\)", output)
    partial_match = re.search(r"PARTIAL MATCH:\s*([\d,]+) rows \(([\d.]+)%\)", output)
    only_match = re.search(r"ONLY:\s*([\d,]+) rows \(([\d.]+)%\)", output)

    if full_match:
        row_counts["full_match"] = int(full_match.group(1).replace(",", ""))
        row_counts["full_match_pct"] = float(full_match.group(2))
    
    if partial_match:
        row_counts["partial_match"] = int(partial_match.group(1).replace(",", ""))
        row_counts["partial_match_pct"] = float(partial_match.group(2))
    
    if only_match:
        row_counts["only_match"] = int(only_match.group(1).replace(",", ""))
        row_counts["only_match_pct"] = float(only_match.group(2))

    # Extract column match percentages (ignoring row count lines)
    column_stats_match = re.findall(r"([\w\d_]+)\s+([\d.]+)", output)
    
    # Convert column stats to DataFrame
    df = pd.DataFrame(column_stats_match, columns=["column", "pct_match"])
    df["pct_match"] = df["pct_match"].astype(float)

    # Remove row count misclassifications (like 'MATCH:' and 'ONLY:')
    df = df[~df["column"].str.contains(r"^(MATCH:|ONLY:)$", regex=True)]

    # Add schema match, row count data, and table name
    df["schema_match"] = schema_match
    df["full_match"] = row_counts["full_match"]
    df["full_match_pct"] = row_counts["full_match_pct"]
    df["partial_match"] = row_counts["partial_match"]
    df["partial_match_pct"] = row_counts["partial_match_pct"]
    df["only_match"] = row_counts["only_match"]
    df["only_match_pct"] = row_counts["only_match_pct"]
    df["table"] = table

    return df

# TODO: tcloud scheduler may not like subprocess

def table_diff(table_dict):
    """Run sqlmesh table diff cli"""

    df_list = []
    for table, grain in table_dict.items():
        if grain:
            command = ["tcloud", "sqlmesh", "table_diff"]
            command.append(f"sqlmesh-public-demo.{table}:tcloud_demo.{table}") # TODO: replace with a table I know exists
            for col in grain:
                command.append('--on')
                command.append(col)
            print(command)
            # run the cli
            result = subprocess.run(command, capture_output=True, text=True).stdout
        else:
            result = 'No grain set'
        # parse result
        df = parse_output_to_df(table, result)
        df_list.append(df)

    return pd.concat(df_list)


@model(
    "utils.table_validation", # TODO: does model path name impact things?
    columns={
            "column": "string",           # Column name from comparison stats
            "pct_match": "float",         # Percentage match for column values
            "schema_match": "bool",       # Boolean for schema match
            "full_match": "float",        # Row count where full match
            "full_match_pct": "float",    # Percentage of rows that fully matched
            "partial_match": "float",     # Row count where partial match
            "partial_match_pct": "float", # Percentage of rows that partially matched
            "only_match": "float",        # Row count only in one table
            "only_match_pct": "float",    # Percentage of rows only in one table
            "table": "string"             # Table name
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
       
    model_dict = {'tcloud_demo__dev.customers': ['customer_id']} # TODO: replace with a table I know exists
    results_df = table_diff(model_dict)
    return results_df