CREATE OR REPLACE FUNCTION logs.log_process_event( --Функция для логгирования данных
    p_table_name VARCHAR,
    p_status VARCHAR,
    p_message TEXT,
    p_start_time TIMESTAMP DEFAULT NULL,
    p_end_time TIMESTAMP DEFAULT NULL
) RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO logs.load_process_log (table_name, start_time, end_time, status, message, duration)
    VALUES (
        p_table_name,
        p_start_time,
        p_end_time,
        p_status,
        p_message,
        CASE
            WHEN p_start_time IS NOT NULL AND p_end_time IS NOT NULL THEN p_end_time - p_start_time --Считаем длительность только если есть время начала и конца
            ELSE NULL
        END
    );
END;
$$;

CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP; -- Время начала выполнения процедуры
    v_end_time TIMESTAMP;   -- Время завершения выполнения процедуры
BEGIN
    -- Логгируем начало работы
    v_start_time := clock_timestamp();
    PERFORM logs.log_process_event(
        'dm_account_turnover_f',
        'STARTED',
        'Процедура запущена',
        v_start_time
    );
    BEGIN
        DELETE FROM dm.dm_account_turnover_f -- Удаляем предыдущие записи для пересчёта
        WHERE on_date = i_OnDate;

        WITH sum_postings_credit AS ( --CTE для высчитывания суммы проводок по кредиту за дату
            SELECT
                balance.account_rk,
                balance.currency_rk,
                SUM(posting.credit_amount) AS credit_amount
            FROM
                ds.ft_balance_f balance
            JOIN
                ds.ft_posting_f posting
                ON balance.account_rk = posting.credit_account_rk
            WHERE
                posting.oper_date = i_OnDate
            GROUP BY
                balance.account_rk, balance.currency_rk
        ),
        sum_postings_debet AS ( -- Сумма проводок по дебету за дату
            SELECT
                balance.account_rk,
                balance.currency_rk,
                SUM(posting.debet_amount) AS debet_amount
            FROM
                ds.ft_balance_f balance
            JOIN
                ds.ft_posting_f posting
                ON balance.account_rk = posting.debet_account_rk
            WHERE
                posting.oper_date = i_OnDate
            GROUP BY
                balance.account_rk, balance.currency_rk
        ),
        union_postings_sum AS ( --Объединяем обе CTE по account_rk для получения итоговой таблицы с суммой проводок по кредиту и дебету
            SELECT
                COALESCE(credit.account_rk, debet.account_rk) AS account_rk,
                COALESCE(credit.currency_rk, debet.currency_rk) AS currency_rk,
                COALESCE(credit.credit_amount, 0) AS credit_amount,
                COALESCE(debet.debet_amount, 0) AS debet_amount
            FROM
                sum_postings_credit credit
            FULL OUTER JOIN
                sum_postings_debet debet
                ON credit.account_rk = debet.account_rk
        ),
        final_calculation AS ( --Финальная CTE для высчитывания суммы в рублях с учётом курса
            SELECT
                ups.account_rk,
                ups.credit_amount,
                ups.debet_amount,
                ups.currency_rk,
                ups.credit_amount * COALESCE(merd.reduced_cource, 1) AS credit_amount_rub,
                ups.debet_amount * COALESCE(merd.reduced_cource, 1) AS debet_amount_rub
            FROM
                union_postings_sum ups
            LEFT JOIN
                ds.md_exchange_rate_d merd
            ON
                merd.currency_rk = ups.currency_rk
                AND merd.data_actual_date = i_OnDate
        )
		--Добавляем итоговые записи в целевую таблицу
        INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
        SELECT
            i_OnDate AS on_date,
            account_rk,
            credit_amount,
            credit_amount_rub,
            debet_amount,
            debet_amount_rub
        FROM final_calculation;

        -- Если всё успешно - логгируем время завершения и статус успешно
        v_end_time := clock_timestamp();
        PERFORM logs.log_process_event(
            'dm_account_turnover_f',
            'COMPLETED',
            'Процедура выполнена успешно',
            v_start_time,
            v_end_time
        );
    EXCEPTION
        WHEN OTHERS THEN
            -- Логгируем ошибку в случае её возникновения
            v_end_time := clock_timestamp();
            PERFORM logs.log_process_event(
                'dm_account_turnover_f',
                'FAILED',
                SQLERRM,
                v_start_time,
                v_end_time
            );
            RAISE;
    END;
END;
$$;


--CALL ds.fill_account_turnover_f('2018-10-01') --Вариант запуска за определённую дату
--Анонимная функция для запуска за период
DO $$
DECLARE
    calc_date DATE := '2018-01-01';
BEGIN
    WHILE calc_date <= '2018-01-31' LOOP
        CALL ds.fill_account_turnover_f(calc_date);
        calc_date := calc_date + INTERVAL '1 day';
    END LOOP;
END;
$$;