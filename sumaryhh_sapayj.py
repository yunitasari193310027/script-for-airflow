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
    dag_id='SumaryHH_SAPAYJ',
    default_args=args,
    ##schedule='0 0 * * *',
    schedule_interval='@once',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
)

# [START howto_operator_bash]
create_command = """
bash /home/hdoop/script_SumaryHH_SAPayj.sh
"""

run_this = BashOperator(
    task_id='taskid1',
    bash_command=create_command,
    dag=dag,
)