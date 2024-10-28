with grouped as (

    select 
        date_trunc('year', transaction_date) as year
        , sum(amount_eur) as networth_delta
        , sum(sum(amount_eur)) over (order by date_trunc('year', transaction_date)) as networth
    from {{ ref('transactions') }}
    group by 1
    order by 1 asc

)

select
    *
    , 100.0 * (networth - lag(networth) over (order by year)) / lag(networth) over (order by year) as percentage_change
from grouped
