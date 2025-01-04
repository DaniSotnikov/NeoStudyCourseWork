insert into DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
SELECT on_date, account_rk, balance_out, balance_out * COALESCE(merd.reduced_cource, 1) AS balance_out_rub
from ds.ft_balance_f balance
         left join ds.md_exchange_rate_d merd on merd.currency_rk = balance.currency_rk
    AND merd.data_actual_date = '2017-12-31'