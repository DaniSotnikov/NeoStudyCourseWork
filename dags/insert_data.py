import logging
from datetime import datetime, timedelta
from imaplib import IMAP4

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.settings import LOG_FORMAT
from sqlalchemy.exc import SQLAlchemyError
import traceback
import pandas as pd
from sqlalchemy import String
from charset_normalizer import detect

logger = logging.getLogger('airflow.task') #Заводим логгер

default_args = {
    'owner': 'dsotnikov',
    'start_date': datetime(2024, 2, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def log_process(table_name: str, status: str, message: str = "") -> tuple:
    """
    Логирует процесс загрузки данных в таблицу.
    :param table_name: Название таблицы
    :param status: Статус выполнения (например, In Progress или Success)
    :param message: Дополнительное сообщение
    :return: log_id (ID записи в логе) и start_time (время начала)
    """
    try:
        postgres_hook = PostgresHook("postgres-db")
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                start_time = datetime.now()
                cursor.execute(
                    """
                    INSERT INTO logs.load_process_log (table_name, start_time, status, message) 
                    VALUES (%s, %s, %s, %s) RETURNING log_id;
                    """,
                    (table_name, start_time, status, message), #Записываем название таблицы, текущее время, статус и сообщение в лог БД
                )
                log_id = cursor.fetchone()[0]
                conn.commit()
                return log_id, start_time
    except Exception as e:
        logger.error(f"Не удалось добавить лог запись в БД: {e}")
        raise


def update_log(log_id: str, status: str, message: str, start_time: datetime) -> None:
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
        postgres_hook = PostgresHook("postgres-db")
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
                conn.commit() #Обновляем лог запись по log_id
    except Exception as e:
        logger.error(f"Не удалось обновить лог запись в БД: {e}")
        raise


def update_currency_code(df: pd.DataFrame) -> pd.DataFrame:
    """
        Преобразует значения столбца CURRENCY_CODE в формат чисел.

        :param df: DataFrame с данными
        :return: Обработанный DataFrame
    """
    not_null_values = df['CURRENCY_CODE'].notna()
    df.loc[not_null_values, 'CURRENCY_CODE'] = df.loc[not_null_values, 'CURRENCY_CODE'].astype(float).astype(
        int).astype(str)
    return df


def detect_encoding(file_path: str) -> str:
    """
        Определяет кодировку файла.
        :param file_path: Путь к файлу
        :return: Кодировка файла
    """
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(1024 * 10) #Читаем не весь фай, а только часть для определения кодировки
        result = detect(raw_data)
        return result['encoding']
    except Exception as e:
        logger.error(f"Не удалось определить кодировку для {file_path}: {traceback.format_exc()}")
        raise


TABLE_PROCESSING_RULES = {
    "ft_balance_f": {
        "deduplication_keys": ["ON_DATE", "ACCOUNT_RK"], #Столбцы подлежащие удалению дубликатов
        "transformations": [], #Список функций для трансформации данных
        "validations": [], #Список функция для валидации
        "type_casting": { #Приведение типов
            "ON_DATE": "date"}
    },
    "ft_posting_f": {
        "deduplication_keys": [],
        "transformations": [],
        "validations": [],
        "type_casting": {
            "OPER_DATE": "date"}
    },
    "md_account_d": {
        "deduplication_keys": ['DATA_ACTUAL_DATE', 'ACCOUNT_RK'],
        "transformations": [],
        "validations": [],
        "type_casting": {
            "DATA_ACTUAL_DATE": "date",
            "DATA_ACTUAL_END_DATE": "date"}
    },
    "md_currency_d": {
        "deduplication_keys": ['CURRENCY_RK', 'DATA_ACTUAL_DATE'],
        "transformations": [
            lambda df: update_currency_code(df) if 'CURRENCY_CODE' in df.columns else None
        ],
        "validations": [],
        "type_casting": {
            "DATA_ACTUAL_DATE": "date",
            "DATA_ACTUAL_END_DATE": "date"}
    },
    "md_exchange_rate_d": {
        "deduplication_keys": ["DATA_ACTUAL_DATE", "CURRENCY_RK"],
        "transformations": [],
        "validations": [],
        "type_casting": {
            "DATA_ACTUAL_DATE": "date",
            "DATA_ACTUAL_END_DATE": "date"}
    },
    "md_ledger_account_s": {
        "deduplication_keys": ["LEDGER_ACCOUNT", "START_DATE"],
        "transformations": [],
        "validations": [],
        "type_casting": {
            "START_DATE": "date",
            "END_DATE": "date"}
    },

}


def process_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
        Обрабатывает данные согласно правилам для конкретной таблицы, правила описаны в TABLE_PROCESSING_RULES.

        :param df: DataFrame с данными
        :param table_name: Название таблицы
        :return: Обработанный DataFrame
    """
    rules = TABLE_PROCESSING_RULES.get(table_name, {}) #определяем набор правил для таблицы

    deduplication_keys = rules.get("deduplication_keys", [])
    if deduplication_keys:
        df = df.drop_duplicates(subset=deduplication_keys) #Удаляем дубликаты

    transformations = rules.get("transformations", [])
    if transformations:
        for transform in transformations: #Вызываем функции для трансформации
            df = transform(df)

    type_casting = rules.get("type_casting", {})
    for column, cast_type in type_casting.items(): #Меняем тип данных
        if column in df.columns:
            if cast_type == "datetime":
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif cast_type == "date":
                df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
            elif cast_type == "numeric":
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif cast_type == "string":
                df[column] = df[column].astype(str)

    validations = rules.get("validations", [])
    if validations:
        for validate in validations: #Вызываем функции -валидаторы
            if not validate(df):
                raise ValueError(f"Validation failed for table {table_name}")

    return df


def insert_data(table_name: str) -> None:
    """
        Основная функция для загрузки данных в таблицу.

        :param table_name: Название таблицы
        """
    log_id, start_time = log_process(table_name, "In Progress", "Loading started")
    try:
        file_path = f'files/{table_name}.csv'
        encoding = detect_encoding(file_path)
        logger.info(f"Обнаружена кодировка для{table_name}: {encoding}")

        df = pd.read_csv(file_path, encoding=encoding, delimiter=';')
        logger.info(f"Успешно прочитан файл{table_name}")

        logger.info(f'Опеределены столбцы в фрейме: {df.columns}')
        df = process_data(df, table_name)
        logger.info(f'Успешно проведена валидация, трансформация, приведение типов и удаление дубликатов')
        df.columns = [col.lower() for col in df.columns]
        logger.info('Привели названия столбцов к нижнему регистру')
        deduplication_keys = TABLE_PROCESSING_RULES[table_name]["deduplication_keys"]
        logger.info(f'Опеределены ключевые столбы для дедупликации: {deduplication_keys}')
        postgres_hook = PostgresHook("postgres-db")
        engine = postgres_hook.get_sqlalchemy_engine()
        logger.info('Успешно создали движок для коннекта к БД')
        temp_table_name = f"temp_{table_name}"
        logger.info(f'Определено название временной таблицы для UPSERT {temp_table_name}')
        with engine.connect() as connection:
            if table_name == 'ft_posting_f':
                df.to_sql(table_name, engine, schema='ds', if_exists='replace', index=False)
            else:
                if table_name == 'md_ledger_account_s':
                    df.to_sql(temp_table_name, connection, schema='ds', if_exists='replace', index=False,
                              dtype={'chapter': String})
                else:
                    df.to_sql(temp_table_name, connection, schema='ds', if_exists='replace', index=False)
                upsert_query = f"""
                INSERT INTO ds.{table_name} ({', '.join(df.columns)})
                SELECT {', '.join(df.columns)} FROM ds.{temp_table_name}
                ON CONFLICT ({', '.join(deduplication_keys)})
                DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in deduplication_keys])};
                """
                logger.info(f'Сгенерировали запрос для добавления записей {upsert_query}')
                connection.execute(upsert_query)
                logger.info(f"UPSERT запрос завершен для {table_name}")

                connection.execute(f"DROP TABLE IF EXISTS ds.{temp_table_name};")
                logger.info('Успешно удалили временную таблицу')
        update_log(log_id, "Success", "Loading completed", start_time)
        logger.info('Обновили лог запись')
    except SQLAlchemyError as e:
        logger.error(f"Ошибка базы данных при загрузке данных для {table_name}: {e}")
        update_log(log_id, "Failed", str(e), start_time)
        raise
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных для{table_name}: {e}")
        update_log(log_id, "Failed", str(e), start_time)
        raise


with DAG(
        'insert_data_from_csv',
        default_args=default_args,
        description='Data loading to ds',
        schedule_interval=None,
        catchup=False,
) as dag:
    tasks = []

    for table in TABLE_PROCESSING_RULES:
        tasks.append(
            PythonOperator(
                task_id=f'load_{table}',
                python_callable=insert_data,
                op_kwargs={'table_name': table},
            )
        )

    tasks
