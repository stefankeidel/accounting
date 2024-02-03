with renamed_columns as (
    select
        id as transaction_id
        , txnidx as tx_index
        , date1 as posting_date
        , status
        , description
        , comment
        , account
        , amount
        , commodity
        , credit
        , debit
        , load_date
        , row_number() over (partition by id order by load_date desc) as row_num
    from {{ source('hledger', 'postings') }}
)

select *
from renamed_columns 
where row_num = 1
