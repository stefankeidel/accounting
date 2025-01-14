select
   name
   , grouping.isin
   , quantity
   , avgcost
   , amount_eur
   , transaction_date
   , class
   , type
   , region
from {{ ref('stg_traderepublic') }} depot
left join {{ ref('traderepublic_grouping') }} grouping
    on depot.isin = grouping.isin
