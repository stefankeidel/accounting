with
    base as (
        select
            transaction_id,
            tx_index,
            posting_date as transaction_date,
            description,
            comment,
            p.account
        from {{ ref("stg_postings") }} p
        join {{ ref("accounts") }} a on p.account = a.account and not a.is_balance_sheet
    )

select *
from base
