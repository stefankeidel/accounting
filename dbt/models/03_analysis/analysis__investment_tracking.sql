select
    *
    , 100.0 * (amount_eur - lag(amount_eur) over (order by transaction_date)) / lag(amount_eur) over (order by transaction_date) as percentage_change
from {{ ref('gls_depot_transactions') }}
