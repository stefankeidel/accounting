select 
    date_trunc('year', transaction_date) as year
    , sum(amount) as networth_delta
    , sum(sum(amount)) over (order by date_trunc('year', transaction_date)) as networth
from {{ ref('transactions') }}
group by 1
order by 1 asc
