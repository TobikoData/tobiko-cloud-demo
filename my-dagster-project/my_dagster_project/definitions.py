from tobikodata.scheduler_facades.dagster import SQLMeshEnterpriseDagster
from dagster import EnvVar # for accessing variables in .env file

# create and configure SQLMeshEnterpriseDagster instance named `sqlmesh`
sqlmesh = SQLMeshEnterpriseDagster(
    url=EnvVar("TOBIKO_CLOUD_BASE_URL").get_value(), # environment variable from .env file
    token=EnvVar("TOBIKO_CLOUD_TOKEN").get_value(), # environment variable from .env file
)

# create Definitions object with `sqlmesh` object's `create_definitions()` method
tobiko_cloud_definitions = sqlmesh.create_definitions(environment="prod")
