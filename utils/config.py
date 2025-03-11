import os
from tobikodata.sqlmesh_enterprise.config.scheduler import CloudSchedulerConfig
from sqlmesh.core.config import (
    Config,
    ModelDefaultsConfig,
    GatewayConfig,
)

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="bigquery"),
    default_gateway="tobiko_cloud",
    gateways={
        "tobiko_cloud": GatewayConfig(
            scheduler = CloudSchedulerConfig(),
        )
    }
)
