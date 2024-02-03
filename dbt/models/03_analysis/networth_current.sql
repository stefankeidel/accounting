select
    account
    , sum(amount)
from {{ ref('transactions') }}
group by 1
having sum(amount) > 0
