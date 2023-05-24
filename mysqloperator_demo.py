import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'hdoop',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['test@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag_mysql = DAG(
    dag_id='mysqloperator_demo',
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    description='use case of mysql operator in airflow',
)

create_sql_query = """ CREATE TABLE yunita.employee(empid int, empname VARCHAR(25), salary int); """

create_table = MySqlOperator(sql=create_sql_query, task_id="CreateTable", mysql_conn_id="mysql_conn", dag=dag_mysql)

insert_sql_query = """ INSERT INTO yunita.employee(empid, empname, salary) VALUES(1,'VAMSHI',30000),(2,'chandu',50000),(3,'chandu',50000); """

insert_data = MySqlOperator(sql=insert_sql_query, task_id="InsertData", mysql_conn_id="mysql_conn", dag=dag_mysql)

delete_sql_query = """ TRUNCATE TABLE yunita.employee; """

delete_data = MySqlOperator(sql=insert_sql_query, task_id="DeleteData", mysql_conn_id="mysql_conn", dag=dag_mysql)

create_table >> delete_data >> insert_data

if __name__ == "__main__":
    dag_mysql.cli()