import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from charset_normalizer import detect
from tenacity import sleep

logger = logging.getLogger(__name__)


logger = logging.getLogger('airflow.task')

default_args = {
    'owner': 'dsotnikov',
    'start_date': datetime(2024, 2, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def log_process(table_name, status, message=""):
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
                    (table_name, start_time, status, message),
                )
                log_id = cursor.fetchone()[0]
                conn.commit()
                return log_id, start_time
    except Exception as e:
        logger.error(f"Failed to log process: {e}")
        raise

def update_log(log_id, status, message, start_time):
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
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to update log: {e}")
        raise

def detect_encoding(file_path):
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(1024 * 10)
        result = detect(raw_data)
        return result['encoding']
    except Exception as e:
        logger.error(f"Failed to detect encoding for {file_path}: {e}")
        raise

TABLE_PROCESSING_RULES = { #TODO дописать кастинги для привеления типов
    "ft_balance_f": {
        "deduplication_keys": ["ON_DATE", "ACCOUNT_RK"],
        "transformations": [],
        "validations": [],
        "type_casting": {
                "ON_DATE": "date"}
    },
    "ft_posting_f": {
        "deduplication_keys": [],
        "transformations": [],
        "validations": []
    },
    "md_account_d": {
        "deduplication_keys": ['DATA_ACTUAL_DATE','ACCOUNT_RK'],
        "transformations": [],
        "validations": []
    },
    "md_currency_d": {
        "deduplication_keys": ['CURRENCY_RK','DATA_ACTUAL_DATE'],
        "transformations": [
            lambda df: df.assign(currency_code=df['CURRENCY_CODE'].apply(
                lambda x: str(int(float(x))) if pd.notna(x) else x
            ))
        ],
        "validations": []
    },
    "md_exchange_rate_d": {
        "deduplication_keys": ["DATA_ACTUAL_DATE", "CURRENCY_RK"],
        "transformations": [],
        "validations": []
    },
    "md_ledger_account_s": {
        "deduplication_keys": ["LEDGER_ACCOUNT", "START_DATE"],
        "transformations": [],
        "validations": []
    },

}

def process_data(df, table_name):
    rules = TABLE_PROCESSING_RULES.get(table_name, {})

    deduplication_keys = rules.get("deduplication_keys", [])
    if deduplication_keys:
        df = df.drop_duplicates(subset=deduplication_keys)

    transformations = rules.get("transformations", [])
    if transformations:
        for transform in transformations:
            df = transform(df)

    type_casting = rules.get("type_casting", {})
    for column, cast_type in type_casting.items():
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
        for validate in validations:
            if not validate(df):
                raise ValueError(f"Validation failed for table {table_name}")

    return df

def insert_data(table_name):
    log_id, start_time = log_process(table_name, "In Progress", "Loading started")
    try:
        file_path = f'files/{table_name}.csv'
        encoding = detect_encoding(file_path)
        logger.info(f"Detected encoding for {table_name}: {encoding}")


        df = pd.read_csv(file_path, encoding=encoding, delimiter=';')
        logger.info(f"Successfully read file {table_name}")


        df = process_data(df, table_name)
        df.columns = [col.lower() for col in df.columns]
        deduplication_keys = TABLE_PROCESSING_RULES[table_name]["deduplication_keys"]

        postgres_hook = PostgresHook("postgres-db")
        engine = postgres_hook.get_sqlalchemy_engine()


        temp_table_name = f"temp_{table_name}"
        with engine.connect() as connection:
            if table_name == 'ft_posting_f':
                df.to_sql(table_name, engine, schema='ds', if_exists='replace', index=False)
            else:

                df.to_sql(temp_table_name, connection, schema='ds', if_exists='replace', index=False)
                logger.info(f"Data loaded into temporary table {temp_table_name}")


                upsert_query = f"""
                INSERT INTO ds.{table_name} ({', '.join(df.columns)})
                SELECT {', '.join(df.columns)} FROM ds.{temp_table_name}
                ON CONFLICT ({', '.join(deduplication_keys)})
                DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in deduplication_keys])};
                """
                connection.execute(upsert_query)
                logger.info(f"UPSERT completed for {table_name}")


                connection.execute(f"DROP TABLE IF EXISTS ds.{temp_table_name};")

        update_log(log_id, "Success", "Loading completed", start_time)

    except SQLAlchemyError as e:
        logger.error(f"Database error during data load for {table_name}: {e}")
        update_log(log_id, "Failed", str(e), start_time)
        raise
    except Exception as e:
        logger.error(f"Error during data load for {table_name}: {e}")
        update_log(log_id, "Failed", str(e), start_time)
        raise


with DAG(
    'insert_data',
    default_args=default_args,
    description='Data loading to staging area',
    schedule_interval='0 0 * * *',
    catchup=False,
) as dag:

    tasks = []
    tables = ['ft_balance_f', 'ft_posting_f', 'md_account_d', 'md_currency_d', 'md_exchange_rate_d', 'md_ledger_account_s']

    for table in tables:
        tasks.append(
            PythonOperator(
                task_id=f'load_{table}',
                python_callable=insert_data,
                op_kwargs={'table_name': table},
            )
        )

    tasks
