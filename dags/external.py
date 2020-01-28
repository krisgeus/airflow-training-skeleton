import datetime

import airflow
from airflow.operators.postgres_to_gcs import PostgresToGCSOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.models import DAG

args = {
    "owner": "Kris",
    "start_date": datetime.datetime(2019,11,24)
}

def check_date(execution_date, **context): 
    return execution_date <= datetime.datetime(2019,11,28)  

with DAG(
    dag_id="exercise-external",
    default_args=args,
    schedule_interval="@daily"
) as dag:

    check_date = ShortCircuitOperator( 
        task_id="check_if_before_end_of_last_year",
        python_callable=check_date, 
        provide_context=True,
    )

    pgsl_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
        bucket="airflow-training-data-land-registry-krisgeus",
        filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
        postgres_conn_id="airflow-training-postgres",
    )

check_date >> psql_to_gcs
