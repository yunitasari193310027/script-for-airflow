from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args={
    'owner': 'hdoop',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='dag_with_taskflow_apiv02',
    default_args=default_args,
    start_date=datetime(2022, 12, 6),
    schedule='@daily')

def hello_world_etl():
    

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Yunita',
            'last_name': 'Sari'
        }

    @task()
    def get_age():
        return 21

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World! My name is {first_name} {last_name}"
            f"and I'm {age} years old!")

    name_dict = get_name()
    age=get_age()
    greet(first_name=name_dict['first_name'],
            last_name=name_dict['last_name'],
            age=age)

greet_dag = hello_world_etl()