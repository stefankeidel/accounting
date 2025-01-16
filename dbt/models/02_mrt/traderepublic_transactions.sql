select
    transaction_id,
    transaction_date::date,
    transaction_type || ' ' || note as description,
    'Assets:Traderepublic Cash' as account,
    amount_eur
from {{ ref("stg_traderepublic_transactions") }}
