import os

from hopper.sqlmesh.plus.loader import SqlMeshPlusLoader
from tobikodata.sqlmesh_enterprise.config.scheduler import RemoteCloudSchedulerConfig
from sqlmesh.core.config import Config, GatewayConfig
from sqlmesh.core.config.categorizer import AutoCategorizationMode
from sqlmesh.core.config.categorizer import CategorizerConfig
from sqlmesh.core.config.common import EnvironmentSuffixTarget
from sqlmesh.core.config.model import ModelDefaultsConfig
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig
from sqlmesh.integrations.github.cicd.config import MergeMethod

config = Config(
    model_defaults=ModelDefaultsConfig(
        dialect="bigquery",
        cron="@hourly",
    ),
    default_gateway="development",
    gateways={
        "development": GatewayConfig(
            scheduler = RemoteCloudSchedulerConfig(),
        )
    },
    default_target_environment=os.getenv("USER", "prod"),
    environment_suffix_target=EnvironmentSuffixTarget.TABLE,
    cicd_bot=GithubCICDBotConfig(
        enable_deploy_command=True,
        merge_method=MergeMethod.SQUASH,
        auto_categorize_changes=CategorizerConfig(
            external=AutoCategorizationMode.FULL,
            python=AutoCategorizationMode.OFF,
            sql=AutoCategorizationMode.FULL,
            seed=AutoCategorizationMode.FULL,
        ),
        pr_include_unmodified=False,
        run_on_deploy_to_prod=False,
        skip_pr_backfill=False,
        default_pr_start="1 day ago",
    ),
    loader=SqlMeshPlusLoader,
)
