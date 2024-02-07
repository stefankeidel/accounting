with cur as (
    select
        account
        , sum(round(cast(amount_eur as numeric),2)) as networth
    from {{ ref('transactions') }}
    group by 1
)

select *
from cur
where networth <> 0
