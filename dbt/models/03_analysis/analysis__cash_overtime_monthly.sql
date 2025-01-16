select
    date_trunc('month', transaction_date),
    sum(round(cast(amount_eur as numeric), 2)) as diff,
    -- rolling sum
    sum(sum(round(cast(amount_eur as numeric), 2))) over (
        order by date_trunc('month', transaction_date)
    ) as balance
from {{ ref("transactions") }}
where account ilike '%giro%'
group by 1
