from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'hdoop',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='SA_PAYJ',
    default_args=args,
    ##schedule='0 0 * * *',
    schedule_interval='@once',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
)

# PART 1
task1 = BashOperator(
    task_id='SA_PAYJ_PART1',
    bash_command="""
        bash /home/hdoop/runer_payj1.sh
    """,
    dag=dag
)

# PART 2
task2 = BashOperator(
    task_id='SA_PAYJ_PART2',
    bash_command="""
        bash /home/hdoop/runer_payj2.sh
    """,
    dag=dag
)

# HH
task3 = BashOperator(
    task_id='SA_PAYJ_HH',
    bash_command="""
        bash /home/hdoop/runer_payj_hh.sh
    """,
    dag=dag
)
# Set task dependencies
task1 >> task2 >> task3