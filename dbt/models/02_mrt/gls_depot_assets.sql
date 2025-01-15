with grouped as (
select
   product_name
   , grouping.wkn
   , quantity
   , piece_eur
   , price_eur
   , load_time
   , class
   , type
   , region
  , -- difference to previous row for transaction ledger
    price_eur - lag(price_eur, 1, 0) over (partition by grouping.wkn order by load_time) as diff
from {{ ref('stg_gls_depot') }} depot
left join {{ ref('gls_assets_grouping') }} grouping
    on depot.wkn  ilike '%' || grouping.wkn || '%'
)

select
  *
  , sum(diff) over (partition by wkn order by load_time) as diff_rolling
from grouped
