select 
    date_trunc('month', transaction_date) as month
    , round(cast(sum(amount_eur) as numeric), 2) as networth_delta
    , round(cast(sum(sum(amount_eur)) over (order by date_trunc('month', transaction_date)) as numeric), 2) as networth
from {{ ref('transactions') }}
group by 1
order by 1 asc
