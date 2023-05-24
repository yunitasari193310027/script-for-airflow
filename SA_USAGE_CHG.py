from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'hdoop',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='SA_USAGE_CHG',
    default_args=args,
    ##schedule='0 0 * * *',
    schedule_interval='@once',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=180),
)

# [START howto_operator_bash]
create_command = """
bash /home/hdoop/scriptSAUsageCHG_Part2.sh
"""

run_this = BashOperator(
    task_id='taskid1',
    bash_command=create_command,
    dag=dag,
)