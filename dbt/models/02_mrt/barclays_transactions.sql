with latest_transaction as (
    select max(posting_date) as max_hledger_date
    from {{ ref('stg_postings') }}
    where account = 'Liabilities:Eurowings Gold'
)

select
    reference as transaction_id
    , value_date as transaction_date
    , description
    , 'Liabilities:Eurowings Gold' as account
    , amount_eur
from {{ ref('stg_barclays') }}
join latest_transaction
    on value_date > max_hledger_date
