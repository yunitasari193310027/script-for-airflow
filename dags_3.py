from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'hdoop',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='DAGS_3',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2022, 12, 5, 1),
    schedule='@hourly'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hey, I am task1"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey, I am task2"
    )

    task3 = BashOperator(
        task_id='thrid_task',
        bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!"
    )

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # Task dependency method 3
    task1 >> [task2, task3]