from tobikodata.scheduler_facades.airflow import SQLMeshEnterpriseAirflow

# this is configured to point at the local tcloud instance created via running:
# $ make cloud-main-up
local = SQLMeshEnterpriseAirflow(conn_id="tobiko_cloud_local")

dag = local.create_cadence_dag(environment="prod")