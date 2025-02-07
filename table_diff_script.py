"""
This script is used to compare the tables in the database and print the differences.
"""
# import sqlmesh to call table diff natively OR use subprocess to call the tcloud table diff command
# if changes are made in plan that are breaking, than diff those tables
# pickup the fully qualified table/view name from the file path
# if the table/view is not in the database, then print an error message
# if the table/view is in the database, then print the differences

# https://sqlmesh.readthedocs.io/en/stable/reference/python/?h=sqlmesh+python

from tobikodata.sqlmesh_enterprise.context import EnterpriseContext

# Initialize the context with your project path and config
context = EnterpriseContext(paths="/Users/sung/Desktop/git_repos/tobiko-cloud-demo/", gateway="tobiko_cloud")

environment = "dev_sung" # TODO: this will be updated with dynamic pr environment name

context_diff = context._context_diff(
    environment=environment, 
)

modified_model_names = {
            *context_diff.modified_snapshots,
            *[s.name for s in context_diff.added],
        }

placeholder_source_database = '`sqlmesh-public-demo`'
placeholder_source_schema = '`tcloud_demo__dev_sung`'

# Create dictionary mapping placeholder names to original names
tables_to_diff = {}
for model_name in modified_model_names:
    # Split the model name into parts
    parts = [part.replace('"', '`') for part in model_name.split('.')]
    if len(parts) == 3:
        # Replace first part with placeholder database
        source_table = f"{placeholder_source_database}.{placeholder_source_schema}.{parts[2]}"
        tables_to_diff[source_table] = f"{parts[0]}.{parts[1]}__{environment}.{parts[2]}"

for k, v in tables_to_diff.items():
    context.table_diff(
        source=k,
        target=v,
        on=["item_id"]
    )


# tcloud sqlmesh table_diff "sqlmesh-public-demo"."tcloud_demo"."stg_payments":"sqlmesh-public-demo"."tcloud_demo__dev_sung"."stg_payments" -o payment_id


# tcloud sqlmesh table_diff prod:dev_sung tcloud_demo.stg_payments -o payment_id


