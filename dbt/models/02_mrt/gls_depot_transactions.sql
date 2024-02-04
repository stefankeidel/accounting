with last_tx_from_ledger as (
    select
        max(posting_date) as transaction_id
        , max(posting_date)::date as transaction_date
        , 'Fake depot to 0 transaction' as description
        , 'Assets:Investments:GLS Depot' as account
        , sum(amount) as amount_eur
    from {{ ref('stg_postings') }} p
    join {{ ref('accounts') }} a
        on p.account = a.account
        and a.is_balance_sheet
    where p.account = 'Assets:Investments:GLS Depot'
)


, all_tx as (

select
    load_time as transaction_id
    , load_time::date as transaction_date
    , 'Some CSV export from depot' as description
    , 'Assets:Investments:GLS Depot' as account
    , sum(price_eur) as amount_eur
from {{ ref('stg_gls_depot') }}
group by 1,2

union all

select
    transaction_id
    , transaction_date
    , description
    , account
    , amount_eur
from last_tx_from_ledger

)
select
   *
   , -- difference to previous row
    amount_eur - lag(amount_eur, 1, amount_eur) over (order by transaction_date) as diff
from all_tx
