import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

WEEKDAY_PERSON_TO_EMAIL = {
    0: "Bob",    # Monday
    1: "Joe",    # Tuesday
    2: "Alice",  # Wednesday
    3: "Joe",    # Thursday
    4: "Alice",  # Friday
    5: "Alice",  # Saturday
    6: "Alice",  # Sunday
}

def _print_weekday(execution_date, **context):
    print(execution_date.strftime("%a"))

# Function returning name of task to execute
def _get_person_to_email(execution_date, **context):
        person = WEEKDAY_PERSON_TO_EMAIL[execution_date.weekday()]
        return f"email_{person.lower()}"


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(14),
}

with DAG(
    dag_id='exercise-branching',
    default_args=args,
    schedule_interval='@daily',
) as dag:

    print_weekday = PythonOperator(
        task_id="print_weekday",
        python_callable=_print_weekday,
        provide_context=True,
    )

    # Branching task, the function above is passed to python_callable.
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=_get_person_to_email,
        provide_context=True,
    )

    # Execute branching task after print_weekday task.
    print_weekday >> branching

    final_task = BashOperator(
        task_id="final_task",
        trigger_rule=TriggerRule.NONE_FAILED,
        bash_command="sleep 5",
    )

    # Create dummy tasks for each person.
    for name in set(WEEKDAY_PERSON_TO_EMAIL.values()):
        email_task = DummyOperator(task_id=f"email_{name.lower()}", dag=dag)
        branching >> email_task >> final_task
