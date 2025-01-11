CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_FromDate DATE;
    v_ToDate DATE;
	v_start_time TIMESTAMP; -- Время начала выполнения процедуры
	v_end_time TIMESTAMP;   -- Время завершения выполнения процедуры
BEGIN
	v_start_time := clock_timestamp();
    PERFORM logs.log_process_event(
        'DM_F101_ROUND_F',
        'STARTED',
        format('Процедура fill_f101_round_f за %s запущена', i_OnDate),
        v_start_time
    );
    -- Определение отчетного периода
    v_FromDate := i_OnDate::DATE - INTERVAL '1 month';
    v_FromDate := DATE_TRUNC('month', v_FromDate);
    v_ToDate := i_OnDate::DATE - INTERVAL '1 day';

    raise notice 'Определены периоды расчёта начиная с % по %', v_FromDate, v_ToDate;

    -- Удаление старых данных за переданную дату
    DELETE FROM DM.DM_F101_ROUND_F
    WHERE FROM_DATE = v_FromDate AND TO_DATE = v_ToDate;

    -- Вставка рассчитанных данных
    INSERT INTO DM.DM_F101_ROUND_F (
        FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC,
        BALANCE_IN_RUB,R_BALANCE_IN_RUB, BALANCE_IN_VAL,R_BALANCE_IN_VAL, BALANCE_IN_TOTAL,R_BALANCE_IN_TOTAL,
        TURN_DEB_RUB,R_TURN_DEB_RUB, TURN_DEB_VAL,R_TURN_DEB_VAL, TURN_DEB_TOTAL,R_TURN_DEB_TOTAL,
        TURN_CRE_RUB,R_TURN_CRE_RUB, TURN_CRE_VAL,R_TURN_CRE_VAL, TURN_CRE_TOTAL, R_TURN_CRE_TOTAL,
        BALANCE_OUT_RUB,R_BALANCE_OUT_RUB, BALANCE_OUT_VAL,R_BALANCE_OUT_VAL, BALANCE_OUT_TOTAL,R_BALANCE_OUT_TOTAL
    )
    WITH
    -- CTE для баланса на начало отчетного периода
    balance_start AS (
        SELECT
            account_rk,
            balance_out_rub
        FROM DM.DM_ACCOUNT_BALANCE_F
        WHERE on_date = v_FromDate - INTERVAL '1 day'
    ),

    -- CTE для оборотов в отчетном периоде
    turnovers AS (
        SELECT
            turn.account_rk,
            SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN debet_amount_rub ELSE 0 END) AS TURN_DEB_RUB,
            SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN debet_amount_rub ELSE 0 END) AS TURN_DEB_VAL,
            SUM(debet_amount_rub) AS TURN_DEB_TOTAL,
            SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN credit_amount_rub ELSE 0 END) AS TURN_CRE_RUB,
            SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN credit_amount_rub ELSE 0 END) AS TURN_CRE_VAL,
            SUM(credit_amount_rub) AS TURN_CRE_TOTAL
        FROM DM.DM_ACCOUNT_TURNOVER_F turn
        JOIN DS.MD_ACCOUNT_D acc ON turn.account_rk = acc.account_rk
        WHERE turn.on_date BETWEEN v_FromDate AND v_ToDate
        GROUP BY turn.account_rk
    ),

    -- CTE для баланса на конец отчетного периода
    balance_end AS (
        SELECT
            account_rk,
            balance_out_rub
        FROM DM.DM_ACCOUNT_BALANCE_F
        WHERE on_date = v_ToDate
    ),

    -- CTE для аккаунтов и связанного справочника
    accounts AS (
        SELECT
            acc.account_rk,
            acc.account_number,
            acc.char_type,
            acc.currency_code
        FROM DS.MD_ACCOUNT_D acc
    )

    -- Основной запрос для вставки данных
    SELECT
        v_FromDate AS FROM_DATE,
        v_ToDate AS TO_DATE,
        ledger.CHAPTER,
        SUBSTRING(acc.account_number FROM 1 FOR 5) AS LEDGER_ACCOUNT,
        acc.char_type AS CHARACTERISTIC,
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN b_start.balance_out_rub ELSE 0 END), 0) AS BALANCE_IN_RUB,
		COALESCE(SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN b_start.balance_out_rub /1000 ELSE 0 END), 0) AS R_BALANCE_IN_RUB,
		COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN b_start.balance_out_rub ELSE 0 END), 0) AS BALANCE_IN_VAL,
		COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN b_start.balance_out_rub / 1000 ELSE 0 END), 0) AS R_BALANCE_IN_VAL,
		COALESCE(SUM(b_start.balance_out_rub), 0) AS BALANCE_IN_TOTAL,
        COALESCE(SUM(b_start.balance_out_rub / 1000), 0) AS R_BALANCE_IN_TOTAL,
		COALESCE(SUM(t.TURN_DEB_RUB), 0) AS TURN_DEB_RUB,
		COALESCE(SUM(t.TURN_DEB_RUB /1000), 0) AS R_TURN_DEB_RUB,
        COALESCE(SUM(t.TURN_DEB_VAL), 0) AS TURN_DEB_VAL,
		COALESCE(SUM(t.TURN_DEB_VAL / 1000), 0) AS R_TURN_DEB_VAL,
        COALESCE(SUM(t.TURN_DEB_TOTAL), 0) AS TURN_DEB_TOTAL,
		COALESCE(SUM(t.TURN_DEB_TOTAL /1000), 0) AS R_TURN_DEB_TOTAL,
        COALESCE(SUM(t.TURN_CRE_RUB), 0) AS TURN_CRE_RUB,
		COALESCE(SUM(t.TURN_CRE_RUB / 1000), 0) AS R_TURN_CRE_RUB,
        COALESCE(SUM(t.TURN_CRE_VAL), 0) AS TURN_CRE_VAL,
		COALESCE(SUM(t.TURN_CRE_VAL / 1000), 0) AS R_TURN_CRE_VAL,
        COALESCE(SUM(t.TURN_CRE_TOTAL), 0) AS TURN_CRE_TOTAL,
		COALESCE(SUM(t.TURN_CRE_TOTAL / 1000), 0) AS R_TURN_CRE_TOTAL,
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END), 0) AS BALANCE_OUT_RUB,
		COALESCE(SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN b_end.balance_out_rub / 1000 ELSE 0 END), 0) AS R_BALANCE_OUT_RUB,
		COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END), 0) AS BALANCE_OUT_VAL,
        COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN b_end.balance_out_rub / 1000 ELSE 0 END), 0) AS R_BALANCE_OUT_VAL,
        COALESCE(SUM(b_end.balance_out_rub), 0) AS BALANCE_OUT_TOTAL,
		COALESCE(SUM(b_end.balance_out_rub) / 1000, 0) AS R_BALANCE_OUT_TOTAL
    FROM accounts acc
    LEFT JOIN DS.MD_LEDGER_ACCOUNT_S ledger ON SUBSTRING(acc.account_number FROM 1 FOR 5) = ledger.ledger_account::TEXT
    LEFT JOIN balance_start b_start ON acc.account_rk = b_start.account_rk
    LEFT JOIN turnovers t ON acc.account_rk = t.account_rk
    LEFT JOIN balance_end b_end ON acc.account_rk = b_end.account_rk
    GROUP BY ledger.CHAPTER, SUBSTRING(acc.account_number FROM 1 FOR 5), acc.char_type;
	v_end_time := clock_timestamp();
        PERFORM logs.log_process_event(
            'DM_F101_ROUND_F',
            'COMPLETED',
            format('Процедура fill_f101_round_f за %s завершена', i_OnDate),
            v_start_time,
            v_end_time
        );
    -- Добавляем запись в таблицу логов об окончании расчета
END;
$$;

-- Выполнение расчета за январь 2018 года
CALL dm.fill_f101_round_f('2018-02-01');
