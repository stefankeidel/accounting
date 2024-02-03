select
    {{ dbt_utils.generate_surrogate_key(['posting_date', 'description', 'p.account', 'amount']) }} as transaction_id
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
