from airflow.operators.python import PythonOperator
from tobikodata.scheduler_facades.airflow import SQLMeshEnterpriseAirflow

def print_success():
    print("Example model completed successfully!")

tobiko_cloud = SQLMeshEnterpriseAirflow(conn_id="tobiko_cloud")

first_task, last_task, dag = tobiko_cloud.create_cadence_dag(environment="prod", dag_kwargs={"schedule": "*/5 * * * *"})

# Get specific model task
example_model = dag.get_task("sqlmesh-public-demo.tcloud_demo.incremental_events_allow_partials")

# Create a task to run after the example_model succeeds
success_task = PythonOperator(
    task_id="run_task_after_model",
    python_callable=print_success,
    dag=dag
)

# Trigger task to run if example_model succeeds
example_model >> success_task
