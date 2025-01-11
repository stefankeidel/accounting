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
from {{ ref('stg_gls_depot') }} depot
left join {{ ref('gls_assets_grouping') }} grouping
    on depot.wkn  ilike '%' || grouping.wkn || '%'
