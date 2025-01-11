CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
	v_start_time TIMESTAMP; -- Время начала выполнения процедуры
	v_end_time TIMESTAMP;   -- Время завершения выполнения процедуры
BEGIN
	v_start_time := clock_timestamp();
    PERFORM logs.log_process_event(
        'dm_account_balance_f',
        'STARTED',
        format('Процедура fill_account_balance_f за %s запущена', i_OnDate),
        v_start_time
    );
    -- Удаляем данные за дату расчета (на случай повторного запуска)
    DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate;

    WITH
    -- Получаем остатки по счетам за предыдущий день
    previous_balance_temp AS (
        SELECT
            account_rk,
            balance_out AS previous_balance_out,
            balance_out_rub AS previous_balance_out_rub
        FROM DM.DM_ACCOUNT_BALANCE_F
        WHERE on_date = i_OnDate::DATE - INTERVAL '1 day'
    ),
    -- Выбираем актуальные курсы валют на дату расчета (Актуальный курс между датами актуальности)
    exchange_rate_temp AS (
        SELECT
		    er.currency_rk,
		    er.reduced_cource
			FROM DS.MD_EXCHANGE_RATE_D er
			WHERE er.data_actual_date <= '2018-01-02'
			  AND (er.data_actual_end_date > '2018-01-02' OR er.data_actual_end_date IS NULL)
    ),
    -- Выбираем актуальные счета на дату расчета
    latest_accounts AS (
        SELECT
            a.account_rk,
            a.currency_rk,
            a.char_type
        FROM DS.MD_ACCOUNT_D a
        WHERE a.data_actual_date <= i_OnDate
          AND (a.data_actual_end_date >= i_OnDate OR a.data_actual_end_date IS NULL)
    ),
    -- Производим расчет остатков
    calculated_balances AS (
        SELECT
            i_OnDate AS on_date,
            a.account_rk,
            -- Расчет остатка в валюте счета
            CASE
			WHEN a.char_type = 'А' THEN COALESCE(pb.previous_balance_out, 0)
                                            + COALESCE(at.debet_amount, 0)
                                            - COALESCE(at.credit_amount, 0)
                WHEN a.char_type = 'П' THEN COALESCE(pb.previous_balance_out, 0)
                                            - COALESCE(at.debet_amount, 0)
                                            + COALESCE(at.credit_amount, 0)
                ELSE 0
            END AS balance_out,
            -- Расчет остатка в рублях
            CASE
                WHEN a.char_type = 'А' THEN (COALESCE(pb.previous_balance_out, 0)
                                            + COALESCE(at.debet_amount, 0)
                                            - COALESCE(at.credit_amount, 0))
                                            * COALESCE(er.reduced_cource, 1)
                WHEN a.char_type = 'П' THEN (COALESCE(pb.previous_balance_out, 0)
                                            - COALESCE(at.debet_amount, 0)
                                            + COALESCE(at.credit_amount, 0))
                                            * COALESCE(er.reduced_cource, 1)
                ELSE 0
            END AS balance_out_rub
        FROM latest_accounts a
        LEFT JOIN previous_balance_temp pb ON a.account_rk = pb.account_rk
        LEFT JOIN dm.dm_account_turnover_f at ON a.account_rk = at.account_rk and on_date = i_OnDate
        LEFT JOIN exchange_rate_temp er ON a.currency_rk = er.currency_rk order by account_rk,on_Date
    )
    -- Вставляем рассчитанные остатки в витрину
    INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        on_date,
        account_rk,
        balance_out,
        balance_out_rub
    FROM calculated_balances;
	v_end_time := clock_timestamp();
        PERFORM logs.log_process_event(
            'dm_account_balance_f',
            'COMPLETED',
            format('Процедура fill_account_balance_f за %s выполнена успешно', i_OnDate),
            v_start_time,
            v_end_time
        );
    -- Добавляем запись в таблицу логов об окончании расчета

    RAISE NOTICE 'Рассчитаны остатки за дату %', i_OnDate;
	EXCEPTION
	WHEN OTHERS THEN
		-- Логгируем ошибку в случае её возникновения
		v_end_time := clock_timestamp();
		PERFORM logs.log_process_event(
			'dm_account_balance_f',
			'FAILED',
			SQLERRM,
			v_start_time,
			v_end_time
		);
            RAISE;
END;
$$;

DO $$
DECLARE
    v_date DATE := '2018-01-01';
BEGIN
    WHILE v_date <= '2018-01-31' LOOP
        CALL ds.fill_account_balance_f(v_date);
        v_date := v_date + INTERVAL '1 day';
    END LOOP;
END;
$$;