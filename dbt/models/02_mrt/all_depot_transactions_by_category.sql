select
   load_time as snapshot_date
   , class
   , type
   , region
   , sum(price_eur) as total_value
from {{ ref('gls_depot_assets') }}
where class is not null
{{ dbt_utils.group_by(n=4) }}

union all

select
    transaction_date as snapshot_date
    , class
    , type
    , region
    , sum(amount_eur) as total_value
from {{ ref('traderepublic_depot_assets') }}
where class is not null
{{ dbt_utils.group_by(n=4) }}
