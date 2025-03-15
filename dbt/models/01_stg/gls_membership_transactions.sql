with tx as (select date, balance from {{ ref("gls_member_balance") }})

select
    date as transaction_date,
    balance,
    -- running difference
    balance - lag(balance, 1, 0) over (order by date) as amount_eur,
    'Assets:Investments:GLS Membership' as account,
    'GLS Membership wrangling' as description,
    'VirtualIncome:Investment Transfers' as category,
    -- generate tx id
    md5(date::text || balance::text) as transaction_id
from tx
