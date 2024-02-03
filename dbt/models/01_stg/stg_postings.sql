with renamed_columns as (
    select
        date1 as posting_date
        , status
        , description
        , comment
        , account
        , amount
        , commodity
        , credit
        , debit
        , load_date
        , row_number() over (partition by date1, description, amount order by load_date desc) as rn
    from {{ source('hledger', 'postings') }}
)

select *
from renamed_columns 
where rn = 1
