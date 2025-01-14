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
where account <> 'Assets:Investments:cosmosdirect BR'

union all

select
    transaction_id::text as transaction_id
    , 'cosmos' as source
    , transaction_date
    , description
    , account
    , amount_eur
    , category
from {{ ref('cosmos_transactions') }}

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

union all

select
    transaction_id
    , 'traderepublic' as source
    , transaction_date
    , name as description
    , account
    , diff as amount_eur
    , null as category
from {{ ref('traderepublic_depot_transactions') }}

)

select
  transaction_id
  , source
  , transaction_date
  , description
  , account
  , round(cast(amount_eur as numeric), 2) as amount_eur
  , category
from unioned
