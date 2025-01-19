select
    t.transaction_id,
    date_trunc('month', t.transaction_date) as month,
    t.source,
    t.transaction_date,
    t.description,
    t.account,
    t.amount_eur,
    coalesce(tm.category, t.category) as category
from {{ ref("transactions") }} t
left join {{ ref("transactions_mapped") }} tm using (transaction_id)
