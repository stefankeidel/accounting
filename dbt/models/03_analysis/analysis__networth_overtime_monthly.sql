select 
    date_trunc('month', transaction_date) as month
    , sum(amount_eur) as networth_delta
    , sum(sum(amount_eur)) over (order by date_trunc('month', transaction_date)) as networth
from {{ ref('transactions') }}
group by 1
order by 1 asc
