from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
default_args = {
    "Owner":"dsotnikov",
    'start_date': datetime(2024, 2, 25),
    'retries': 2,
}

with DAG('insert_data', default_args=default_args,description='Загрузка данных в stage',catchup=False, schedule='0 0 * * *') as dag:
    start = DummyOperator(
        task_id='start',
    )
    end = DummyOperator(task_id='end')

    (start >> end)