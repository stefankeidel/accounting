with base as (
    select
        transaction_id
        , tx_index
        , posting_date as transaction_date
        , description
        , comment
        , p.account
        , commodity
        , amount
    from {{ ref('stg_postings') }} p
    join {{ ref('accounts') }} a
        on p.account = a.account
        and a.is_balance_sheet
)

select distinct
    b.*
    , c.account as category
from base b
left join {{ ref('transaction_categories') }} c
   using (tx_index)
