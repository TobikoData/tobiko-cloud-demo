from tobikodata.scheduler_facades.dagster import SQLMeshEnterpriseDagster
from dagster import (
    EnvVar,
    AssetKey,
    SensorEvaluationContext,
    EventLogEntry,
    job,
    asset_sensor,
    RunRequest,
    Definitions,
)  # for accessing variables in .env file

# create and configure SQLMeshEnterpriseDagster instance named `sqlmesh`
sqlmesh = SQLMeshEnterpriseDagster(
    url=EnvVar("TOBIKO_CLOUD_BASE_URL").get_value(), # get the base url from the environment variable
    token=EnvVar("TOBIKO_CLOUD_TOKEN").get_value(), # get the token from the environment variable
    dagster_graphql_host="localhost",  # Example GraphQL host (could be passed in an environment variable instead)
    dagster_graphql_port=3000,  # Example GraphQL port (could be passed in an environment variable instead)
)


# define a job to run when the asset is updated
@job
def internal_customers_pipeline():
    # custom logic goes here
    print("customers updated")


@asset_sensor(
    asset_key=AssetKey(
        ["sqlmesh-public-demo", "tcloud_demo", "customers"]
    ),  # Asset key found in Dagster Asset Catalog
    job=internal_customers_pipeline,
)
def on_customers_updated(context: SensorEvaluationContext, asset_event: EventLogEntry):
    yield RunRequest()


# merge existing and sqlmesh definitions
defs = Definitions(jobs=[internal_customers_pipeline], sensors=[on_customers_updated])

defs = Definitions.merge(defs, sqlmesh.create_definitions(environment="prod"))
