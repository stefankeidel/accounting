with accounts as (

    select distinct
       account
       , case
           when account ilike '%assets%' then 1
           when account ilike '%liabilities%' then -1
           else 0 -- categories, like Expenses and Income
         end as sign
    from {{ ref('stg_postings') }}

)

select
    *
    , sign <> 0 as is_balance_sheet
from accounts
