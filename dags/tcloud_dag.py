from tobikodata.scheduler_facades.airflow import SQLMeshEnterpriseAirflow

tobiko_cloud = SQLMeshEnterpriseAirflow(conn_id="tobiko_cloud")

first_task, last_task, dag = tobiko_cloud.create_cadence_dag(environment="prod")