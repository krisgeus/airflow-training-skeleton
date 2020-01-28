import datetime

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Kris',
    'start_date': datetime.datetime(2020,1,25),
}

with DAG(
    dag_id='exercise-first-dag',
    default_args=args,
    schedule_interval='@daily',
) as dag:

    task1 = DummyOperator(task_id="task1")
    task2 = DummyOperator(task_id="task2")
    task3 = DummyOperator(task_id="task3")
    task4 = DummyOperator(task_id="task4")
    task5 = DummyOperator(task_id="task5")

    task1 >> task2 >> [task3, task4] >> task5
