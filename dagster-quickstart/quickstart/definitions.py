from tobikodata.scheduler_facades.dagster import SQLMeshEnterpriseDagster
import dagster as dg

# create and configure SQLMeshEnterpriseDagster instance named `sqlmesh`
sqlmesh = SQLMeshEnterpriseDagster(
    url=dg.EnvVar(
        "TOBIKO_CLOUD_BASE_URL"
    ).get_value(),  # get the base url from the environment variable
    token=dg.EnvVar(
        "TOBIKO_CLOUD_TOKEN"
    ).get_value(),  # get the token from the environment variable
    dagster_graphql_host="localhost",  # Example GraphQL host (could be passed in an environment variable instead)
    dagster_graphql_port=3000,  # Example GraphQL port (could be passed in an environment variable instead)
)


# define a job to run when the asset is updated
@dg.op
def log_customers_updated(context):
    context.log.info("customers updated")


@dg.job
def internal_customers_pipeline():
    log_customers_updated()


@dg.asset_sensor(
    asset_key=dg.AssetKey(
        ["sqlmesh-public-demo", "tcloud_demo", "incremental_events_allow_partials"]
    ),  # Asset key found in Dagster Asset Catalog
    job=internal_customers_pipeline,
)
def on_customers_updated(
    context: dg.SensorEvaluationContext, asset_event: dg.EventLogEntry
):
    yield dg.RunRequest()


# merge existing and sqlmesh definitions
defs = dg.Definitions(
    jobs=[internal_customers_pipeline], sensors=[on_customers_updated]
)

defs = dg.Definitions.merge(defs, sqlmesh.create_definitions(environment="prod"))
