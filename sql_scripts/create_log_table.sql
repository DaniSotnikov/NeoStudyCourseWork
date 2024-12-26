-- Создаем схему для логов
CREATE SCHEMA IF NOT EXISTS logs;

-- Создаем таблицу для логов
CREATE TABLE IF NOT EXISTS logs.load_process_log (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255),
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    status VARCHAR(50),     
    message TEXT,
    duration INTERVAL
);