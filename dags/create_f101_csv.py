import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import traceback
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd


logger = logging.getLogger('airflow.task')

default_args = {
    'owner': 'dsotnikov',
    'start_date': datetime(2024, 2, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def log_process(table_name: str, status: str, message: str = '') -> tuple:
    """
    Логирует процесс выгрузки данных в csv.
    :param table_name: Название таблицы
    :param status: Статус выполнения (например, In Progress или Success)
    :param message: Дополнительное сообщение
    :return: log_id (ID записи в логе) и start_time (время начала)
    """
    try:
        postgres_hook = PostgresHook('postgres-db')
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                start_time = datetime.now()
                cursor.execute(
                    """
                    INSERT INTO logs.load_process_log (table_name, start_time, status, message) 
                    VALUES (%s, %s, %s, %s) RETURNING log_id;
                    """,
                    (table_name, start_time, status, message),
                    # Записываем название таблицы, текущее время, статус и сообщение в лог БД
                )
                log_id = cursor.fetchone()[0]
                conn.commit()
                return log_id, start_time
    except Exception as e:
        logger.error(f'Не удалось добавить лог запись в БД: {e}')
        raise


def update_log(
    log_id: str, status: str, message: str, start_time: datetime
) -> None:
    """
    Обновляет запись в логе после завершения процесса.

    :param log_id: ID записи в логе
    :param status: Статус выполнения (например, "Success" или "Failed")
    :param message: Сообщение о статусе
    :param start_time: Время начала процесса
    """
    try:
        end_time = datetime.now()
        duration = end_time - start_time
        postgres_hook = PostgresHook('postgres-db')
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE logs.load_process_log 
                    SET end_time = %s, status = %s, message = %s, duration = %s 
                    WHERE log_id = %s;
                    """,
                    (end_time, status, message, duration, log_id),
                )
                conn.commit()  # Обновляем лог запись по log_id
    except Exception as e:
        logger.error(f'Не удалось обновить лог запись в БД: {e}')
        raise


def create_f101_to_csv() -> None:
    """
    Основная функция для выгрузки 101 формы в csv.
    """
    log_id = None
    start_time = None
    try:
        log_id, start_time = log_process(
            'dm.DM_F101_ROUND_F',
            'In Progress',
            'Create csv f101_round started',
        )
        postgres_hook = PostgresHook('postgres-db')
        logger.info('Создали хук для коннекта к БД')
        engine = postgres_hook.get_sqlalchemy_engine()
        logger.info('Создали движок для работы с БД')
        with engine.connect() as connection:
            logger.info('Установлено соединение с БД')
            result = connection.execute('SELECT * FROM dm.DM_F101_ROUND_F;')
            records = result.fetchall()
            if not records:
                logger.warning(
                    'Запрос не вернул данных. Файл CSV создан не будет.'
                )
                update_log(
                    log_id, 'No Data', 'Query returned no data', start_time
                )
                return
            columns = result.keys()
            logger.info(f'Определены названия колонок {columns}')
            logger.info(f'Получено {len(records)} записей из таблицы')
            df = pd.DataFrame(records, columns=columns)
            logger.info(f'Результат запроса преобразован в DataFrame')
        dags_folder = os.path.dirname(os.path.abspath(__file__))  # Папка dags
        logger.info(f'Определили путь папки dags {dags_folder}')
        files_folder = os.path.join(
            dags_folder, '..', 'files'
        )  # Путь к папке files
        logger.info(f'Определили путь папки files {files_folder}')
        csv_file_path = os.path.join(files_folder, 'f101_round.csv')
        logger.info(
            f'Сгенерировали путь и название итогового csv {csv_file_path}'
        )
        df.to_csv(csv_file_path, index=False, sep=';', encoding='utf-8')
        logger.info('Успешно создали итоговый файл')
        update_log(
            log_id, 'Success', 'Create csv f101_round completed', start_time
        )
    except SQLAlchemyError as e:
        logger.error(
            f'Ошибка базы данных при выгрузке данных из dm.DM_F101_ROUND_F в csv: {traceback.format_exc()}'
        )
        if log_id and start_time:
            update_log(log_id, 'Failed', str(e), start_time)
        raise
    except Exception as e:
        logger.error(
            f'Ошибка при выгрузке данных из dm.DM_F101_ROUND_F в csv: {traceback.format_exc()}'
        )
        if log_id and start_time:
            update_log(log_id, 'Failed', str(e), start_time)
        raise


with DAG(
    'create_f101_to_csv',
    default_args=default_args,
    description='create_f101_to_csv',
    schedule_interval=None,
    catchup=False,
) as dag:
    fetch_records_task = PythonOperator(
        task_id='create_f101_to_csv', python_callable=create_f101_to_csv
    )
