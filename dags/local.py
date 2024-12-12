from tobikodata.scheduler_facades.airflow import SQLMeshEnterpriseAirflow

local = SQLMeshEnterpriseAirflow(conn_id="tobiko_cloud_local")

dag = local.create_cadence_dag(environment="prod")