import os
import json
from sqlmesh.core.config import (
    GatewayConfig,
    ModelDefaultsConfig,
    BigQueryConnectionConfig,
)
from tobikodata.sqlmesh_enterprise.config import (
    EnterpriseConfig,
    RemoteCloudSchedulerConfig,
)

config = EnterpriseConfig(
    default_gateway="tobiko_cloud",  # name in tcloud ui connections page
    model_defaults=ModelDefaultsConfig(dialect="bigquery"),
    gateways={
        "tobiko_cloud": GatewayConfig(
            scheduler=RemoteCloudSchedulerConfig(),
            connection=BigQueryConnectionConfig(  # used to create unit tests and external models
                type="bigquery",
                method="service-account-json",
                concurrent_tasks=5,
                register_comments=True,
                keyfile_json=json.loads(os.environ["GOOGLE_SQLMESH_CREDENTIALS"]),
                project="sqlmesh-public-demo",
            ),
        ),
    },
)
