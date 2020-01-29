import airflow
from airflow.models import DAG
from operators.launch_operator import LaunchToGcsOperator

args = {
    "owner": "Kris",
    "start_date": airflow.utils.dates.days_ago(10)
}

dag = DAG(
    dag_id="download_rocket_launches_operator",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *"
)

download_rocket_launches = LaunchToGcsOperator(
    task_id='get_rocket_launches',
    start_date='{{ds}}',
    output_bucket="airflow-training-data-land-registry-krisgeus",
    output_path="launches/{{ ds }}/launches.json",
    end_date='{{ tomorrow_ds }}',
    launch_conn_id='launches_connection',
    dag=dag
)

download_rocket_launches
