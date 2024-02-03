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
    from {{ source('hledger', 'postings') }}
)

select *
from renamed_columns 
