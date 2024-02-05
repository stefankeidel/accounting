with unioned as (

select
    transaction_id
    , 'barclays' as source
    , transaction_date
    , description
    , account
    , amount_eur
    , null as category
from {{ ref('barclays_transactions') }}

union all

select
    transaction_id::text as transaction_id
    , 'hledger' as source
    , transaction_date
    , description
    , account
    , amount as amount_eur
    , category
from {{ ref('hledger_transactions') }}

union all

select
    transaction_id
    , 'gls' as source
    , transaction_date
    , description
    , account
    , amount_eur
    , null as category
from {{ ref('gls_transactions') }}

union all

select
    transaction_id::text as transaction_id
    , 'gls_depot' as source
    , transaction_date
    , description
    , account
    , diff as amount_eur
    , null as category
from {{ ref('gls_depot_transactions') }}

)

select *
from unioned
