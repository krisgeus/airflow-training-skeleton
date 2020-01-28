import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def _print_execution_date(execution_date, **_):
    print(execution_date)


args = {
    'owner': 'Kris',
    'start_date': airflow.utils.dates.days_ago(7),
}

with DAG(
    dag_id='exercise-templating',
    default_args=args,
    schedule_interval='@daily',
) as dag:

    print_execution_date = PythonOperator(
        task_id="print_execution_date",
        python_callable=_print_execution_date,
        provide_context=True
    )

    the_end = DummyOperator(task_id="the_end")

    for i in (1, 5, 10):
        wait = BashOperator(
            task_id=f"wait_{i}",
            bash_command=f"sleep {i}"
        )

        print_execution_date >> wait >> the_end
