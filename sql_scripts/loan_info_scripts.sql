--Создаём временные таблицы для отладки

CREATE TABLE temp.dict_currency AS TABLE dm.dict_currency;

CREATE TABLE temp.deal_info AS TABLE rd.deal_info;

CREATE TABLE temp.product AS TABLE rd.product;

--Данные с CSV записываем в таблицу с помощью psql

\copy temp_product FROM 'Путь до файла' DELIMITER ',' CSV HEADER;

\copy temp_deal_info FROM 'Путь до файла' DELIMITER ',' CSV HEADER;

SELECT * FROM temp.temp_product -- получаем все записи которые есть в csv и которых нет в источнике
EXCEPT
SELECT * FROM rd.product;

SELECT * FROM rd.product --Получаем все записи, которые есть в источнике, но нет в csv
EXCEPT
SELECT * FROM temp.temp_product;

SELECT * FROM temp.deal_info -- Получаем все записи, которые есть в csv, но нет в источнике
EXCEPT
SELECT * FROM rd.deal_info;

SELECT * FROM rd.deal_info -- Получаем все записи, которые есть в источнике, но нет в csv
EXCEPT
SELECT * FROM temp.deal_info order by effective_from_date;

SELECT * FROM dm.dict_currency -- Получаем все записи, которые есть в источнике, но нет в csv
EXCEPT
SELECT * FROM temp.dict_currency order by effective_from_date;

SELECT * FROM temp.dict_currency -- Получаем все записи, которые есть в источнике, но нет в csv
EXCEPT
SELECT * FROM dm.dict_currency order by effective_from_date;

--Добавление в product недостающих записей

INSERT INTO rd.product (product_rk, product_name, effective_from_date, effective_to_date)
SELECT product_rk, product_name, effective_from_date, effective_to_date
FROM temp.temp_product
EXCEPT
SELECT product_rk, product_name, effective_from_date, effective_to_date
FROM rd.product;

--Добавление в deal_info недостающих записей

INSERT INTO rd.deal_info (deal_rk,deal_num,deal_name,deal_sum,client_rk,account_rk,agreement_rk,deal_start_date,department_rk,product_rk,deal_type_cd,effective_from_date,effective_to_date)
SELECT deal_rk,deal_num,deal_name,deal_sum,client_rk,account_rk,agreement_rk,deal_start_date,department_rk,product_rk,deal_type_cd,effective_from_date,effective_to_date
FROM temp.deal_info
EXCEPT
SELECT deal_rk,deal_num,deal_name,deal_sum,client_rk,account_rk,agreement_rk,deal_start_date,department_rk,product_rk,deal_type_cd,effective_from_date,effective_to_date
FROM rd.deal_info;

--Добавление в dm.dict_currency недостающих записей
INSERT INTO dm.dict_currency (currency_cd, currency_name, effective_from_date,effective_to_date)
SELECT currency_cd, currency_name, effective_from_date,effective_to_date
FROM temp.dict_currency
EXCEPT
SELECT currency_cd, currency_name, effective_from_date,effective_to_date
FROM dm.dict_currency;

--Процедура для пересчёта витрины

CREATE OR REPLACE PROCEDURE refresh_loan_holiday_info()
LANGUAGE plpgsql
AS $$
BEGIN
truncate table dm.loan_holiday_info;
with deal as (
  select
    deal_rk,
    deal_num,
    deal_name,
    deal_sum,
    client_rk,
	account_rk,
    agreement_rk,
    deal_start_date,
    department_rk,
    product_rk,
    deal_type_cd,
    effective_from_date,
    effective_to_date
  from
    rd.deal_info
),
loan_holiday as (
  select
    deal_rk,
    loan_holiday_type_cd,
    loan_holiday_start_date,
    loan_holiday_finish_date,
    loan_holiday_fact_finish_date,
    loan_holiday_finish_flg,
    loan_holiday_last_possible_date,
    effective_from_date,
    effective_to_date
  from
    rd.loan_holiday
),
product as (
  select
    product_rk,
    product_name,
    effective_from_date,
    effective_to_date
  from
    rd.product
),
holiday_info as (
  select
    d.deal_rk,
    lh.effective_from_date,
    lh.effective_to_date,
    d.deal_num as deal_number,
    lh.loan_holiday_type_cd,
    lh.loan_holiday_start_date,
    lh.loan_holiday_finish_date,
    lh.loan_holiday_fact_finish_date,
    lh.loan_holiday_finish_flg,
    lh.loan_holiday_last_possible_date,
    d.deal_name,
    d.deal_sum,
	d.account_rk,
    d.client_rk,
    d.agreement_rk,
    d.deal_start_date,
    d.department_rk,
    d.product_rk,
    p.product_name,
    d.deal_type_cd
  from
    deal d
    left join loan_holiday lh on 1 = 1
    and d.deal_rk = lh.deal_rk
    and d.effective_from_date = lh.effective_from_date
    left join product p on p.product_rk = d.product_rk
    and p.effective_from_date = d.effective_from_date
) INSERT INTO dm.loan_holiday_info
SELECT
  deal_rk,
  effective_from_date,
  effective_to_date,
  agreement_rk,
  account_rk,
  client_rk,
  department_rk,
  product_rk,
  product_name,
  deal_type_cd,
  deal_start_date,
  deal_name,
  deal_number,
  deal_sum,
  loan_holiday_type_cd,
  loan_holiday_start_date,
  loan_holiday_finish_date,
  loan_holiday_fact_finish_date,
  loan_holiday_finish_flg,
  loan_holiday_last_possible_date
FROM
  holiday_info;
RAISE NOTICE 'Процедура пересчёта данных выполнена loan_holiday_info успешно.';
END;
$$;
CALL refresh_loan_holiday_info()
