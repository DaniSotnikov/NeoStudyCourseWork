from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import time

logger = logging.getLogger('airflow.task')

default_args = {
    "Owner": "dsotnikov",
    'start_date': datetime(2024, 2, 25),
    'retries': 2,
}


def log_process(table_name, status, message=""):
    postgres_hook = PostgresHook("postgres-db")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Логирование начала загрузки
    start_time = datetime.now()
    cursor.execute("""
        INSERT INTO logs.load_process_log (table_name, start_time, status, message)
        VALUES (%s, %s, %s, %s) RETURNING log_id;
    """, (table_name, start_time, status, message))
    log_id = cursor.fetchone()[0]
    conn.commit()

    return log_id, start_time


def update_log(log_id, status, message, start_time):
    end_time = datetime.now()
    duration = end_time - start_time
    postgres_hook = PostgresHook("postgres-db")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE logs.load_process_log 
        SET end_time = %s, status = %s, message = %s, duration = %s 
        WHERE log_id = %s;
    """, (end_time, status, message, duration, log_id))
    conn.commit()


def insert_data(table_name):
    logger.info(f'Начат процесс загрузки данных из {table_name}')

    # Логируем начало процесса
    log_id, start_time = log_process(table_name, "В процессе", "Загрузка данных началась")

    # Чтение CSV файла
    df = pd.read_csv(f'files/{table_name}.csv', encoding="cp1251", delimiter=';')
    logger.info('Успешно прочитали файл')

    # Преобразование имен столбцов в нижний регистр
    df.columns = [col.lower() for col in df.columns]

    # Задержка на 5 секунд
    time.sleep(5)

    # Создание соединения с базой данных
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Загрузка данных в таблицу
    df.to_sql(table_name, engine, schema='ds', if_exists='append', index=False)
    logger.info(f'Данные успешно добавлены в таблицу {table_name}')

    # Обновляем лог завершения
    update_log(log_id, "Завершено успешно", "Загрузка данных завершена", start_time)


with DAG('insert_data', default_args=default_args, description='Загрузка данных в stage', catchup=False,
         schedule='0 0 * * *') as dag:
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
