from sqlmesh.core.config import GatewayConfig, ModelDefaultsConfig

from tobikodata.sqlmesh_enterprise.config import EnterpriseConfig, RemoteCloudSchedulerConfig

config = EnterpriseConfig(
    default_gateway="tobiko_cloud", # name in tcloud ui connections page
    model_defaults=ModelDefaultsConfig(dialect="bigquery"),
    gateways={
        "tobiko_cloud": GatewayConfig(
            scheduler=RemoteCloudSchedulerConfig()
        ),
    },
)
