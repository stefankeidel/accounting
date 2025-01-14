with all_asset_tx as (
   select
     load_time as snapshot_date
      , date_trunc('month', load_time) as snapshot_month
      , wkn as id
      , 'gls' as source
      , class
      , type
      , region
      , price_eur
   from {{ ref('gls_depot_assets') }}
   where class is not null

   union all

   select
       transaction_date as snapshot_date
       , date_trunc('month', transaction_date) as snapshot_month
       , isin as id
       , 'traderepublic'as source
       , class
       , type
       , region
       , amount_eur as price_eur
   from {{ ref('traderepublic_depot_assets') }}
   where class is not null
)

select
  *
  , row_number() over (partition by id, source, month order by snapshot_date desc) as rn
from all_asset_tx

-- todo: join against a list of months and always take what is closest to the month or the latest in that month
