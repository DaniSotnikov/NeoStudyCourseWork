from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

logger = logging.getLogger('airflow.task')

default_args = {
    "Owner":"dsotnikov",
    'start_date': datetime(2024, 2, 25),
    'retries': 2,
}

with DAG('insert_data', default_args=default_args,description='Загрузка данных в stage',catchup=False, schedule='0 0 * * *') as dag:
    def insert_data(table_name):
        logger.info(f'Начат процесс загрузки данных из {table_name}')
        df = pd.read_csv(f'files/{table_name}.csv',encoding="cp1251",delimiter=';')
        logger.info('Успешно прочитали файл')
        logger.info(df.columns)
        postgres_hook = PostgresHook("postgres-db")
        engine = postgres_hook.get_sqlalchemy_engine()
        df.to_sql(table_name, engine, schema='ds',if_exists='append', index=False)


    ft_balance_f = PythonOperator(task_id='ft_balance_f', python_callable=insert_data,
                                  op_kwargs={'table_name': 'ft_balance_f'})

    ft_posting_f = PythonOperator(task_id='ft_posting_f', python_callable=insert_data,
                                  op_kwargs={'table_name': 'ft_posting_f'})

    md_account_d = PythonOperator(task_id='md_account_d', python_callable=insert_data,
                                  op_kwargs={'table_name': 'md_account_d'})

    md_currency_d = PythonOperator(task_id='md_currency_d', python_callable=insert_data,
                                  op_kwargs={'table_name': 'md_currency_d'})

    md_exchange_rate_d = PythonOperator(task_id='md_exchange_rate_d', python_callable=insert_data,
                                   op_kwargs={'table_name': 'md_exchange_rate_d'})

    md_ledger_account_s = PythonOperator(task_id='md_ledger_account_s', python_callable=insert_data,
                                        op_kwargs={'table_name': 'md_ledger_account_s'})

    ft_balance_f >> ft_posting_f >> md_account_d >> md_currency_d >> md_exchange_rate_d >> md_ledger_account_s