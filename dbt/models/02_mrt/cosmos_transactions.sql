with old_tx as (

    select
        transaction_date as date
        , amount
        , sum(amount) over (order by transaction_date) as balance
    from {{ ref('hledger_transactions') }}
    where account = 'Assets:Investments:cosmosdirect BR'

)

, new_tx as (
    select
       date
       , balance
    from {{ ref('cosmos_balance') }}
)

, all_tx as (
    select
       date
       , balance
    from old_tx
    union all
    select
      date
      , balance
    from new_tx
)

select
   date as transaction_date
   , balance
   -- running difference
   , balance - lag(balance, 1, 0) over (order by date) as amount_eur
   , 'Assets:Investments:cosmosdirect BR' as account
   , 'CosmosDirect wrangling' as description
   , 'VirtualIncome:Investment Transfers' as category
   -- generate tx id
    , md5(date::text || balance::text) as transaction_id
from all_tx
