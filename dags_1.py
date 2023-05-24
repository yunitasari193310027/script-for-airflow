# step 1

from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.empty import EmptyOperator

# step 2
default_args = {
'owner': 'hdoop',
'depends_on_past': False,
'start_date': datetime(2022, 12, 5),
'retries': 0
}

# step 3 : creating dag object
##dag = DAG(dag_id='DAG-1', default_args=default_args, catchup=False, schedule='@once')
dag = DAG(dag_id='DAG-1', default_args=default_args, catchup=False, schedule='@daily')
##dag = DAG(dag_id='DAG-1', default_args=default_args, catchup=False, schedule='@hourly')

# step 4
dummy_start = EmptyOperator(task_id='dummy_start', dag=dag)
dummy_end = EmptyOperator(task_id='dummy_end', dag=dag)

# step 5
dummy_start >> dummy_end
