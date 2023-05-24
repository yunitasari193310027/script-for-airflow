import csv
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'hdoop',
    'depens_on_past': False,
    'retries':5,
    'retry_delay': timedelta(minutes=10)
}

def postgres_to_s3():
    hook = PostgresHook(postgres_conn_id="postgres_local")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date <= '20220501'")
    with open("/home/hdoop/airflow/dags/get_orders.txt","w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)
    cursor.close()
    conn.close()
    logging.info("saved orders data in text file fet_orders.txt")
    
        

with DAG(
    dag_id='hooks_postgres',
    default_args=default_args,
    start_date=datetime(2022, 12, 6),
    schedule_interval='@daily' 
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1
    