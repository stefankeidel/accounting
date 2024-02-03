with latest_transaction as (
    select max(posting_date) as max_hledger_date
    from {{ ref('stg_postings') }}
    where account = 'Assets:GLS Giro'
)

select
    transaction_id
    , value_date as transaction_date
    , name_zahlungsbeteiligter || ' ' || verwendungszweck as description
    , 'Assets:GLS Giro' as account
    , amount_eur
from {{ ref('stg_gls') }}
join latest_transaction
    on value_date > max_hledger_date
