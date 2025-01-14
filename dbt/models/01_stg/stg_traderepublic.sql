select
    name
    , isin
    , quantity
    , avgcost
    , netvalue as amount_eur
    , load_time as transaction_date
    , {{ dbt_utils.generate_surrogate_key(['isin', 'load_time']) }} as transaction_id
from {{ source('public', 'traderepublic') }}
