select
    transaction_id
    , 'barclays' as source
    , transaction_date
    , description
    , account
    , amount_eur
from {{ ref('barclays_transactions') }}

union all

select
    transaction_id::text as transaction_id
    , 'hledger' as source
    , transaction_date
    , description
    , account
    , amount as amount_eur
from {{ ref('hledger_transactions') }}

union all

select
    transaction_id
    , 'gls' as source
    , transaction_date
    , description
    , account
    , amount_eur
from {{ ref('gls_transactions') }}
