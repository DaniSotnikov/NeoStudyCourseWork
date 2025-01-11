SELECT * FROM ds.md_account_d where account_number like '30233%' --Список аккаунтов по 30233 порядку
ORDER BY data_actual_date ASC, account_rk ASC

SELECT sum(balance_out),sum(balance_out_rub) FROM dm.dm_account_balance_f where account_rk in ( --Сумма остатков по всем кроме рублёвых
18006,
44195) and on_date = '2017-12-31'

SELECT sum(balance_out),sum(balance_out_rub) FROM dm.dm_account_balance_f where account_rk in (18007) and on_date = '2017-12-31' -- сумма остатков по рублёвым (он 1 такой)

SELECT sum(balance_out) sum_balance_out,sum(balance_out_rub) sum_balance_out_rub FROM dm.dm_account_balance_f where account_rk
in (18007,18006,44195) and on_date = '2017-12-31' --Сумма остатков на начало по всем счетам

SELECT sum(debet_amount_rub),sum(credit_amount_rub)FROM dm.dm_account_turnover_f where account_rk
in (18006,44195) -- сумма оборотов за весь период по не рублёвым

SELECT sum(debet_amount_rub),sum(credit_amount_rub)FROM dm.dm_account_turnover_f where account_rk
in (18007) -- сумма оборотов за весь период по рублёвым

SELECT sum(debet_amount_rub),sum(credit_amount_rub)FROM dm.dm_account_turnover_f where account_rk
in (18007,18006,44195) -- сумма оборотов за весь период по всем

select * from dm.dm_account_balance_f where on_date = '2018-01-31' and account_rk in(18007) order by account_rk,on_date -- остатки на последний день по рублёвым

select sum(balance_out),sum(balance_out_rub) from dm.dm_account_balance_f where on_date = '2018-01-31' and account_rk in(18006,44195) -- остатки на последний день по не рублёвым

select sum(balance_out),sum(balance_out_rub) from dm.dm_account_balance_f where on_date = '2018-01-31' and account_rk in(18006,18007,44195) -- остатки на последний день по Всем


