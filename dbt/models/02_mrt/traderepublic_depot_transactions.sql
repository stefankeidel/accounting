select
    depot.*,
    'Assets:Investments:TradeRepublic' as account,  -- difference to previous row for transaction ledger
    amount_eur - lag(amount_eur, 1, 0) over (
        partition by depot.isin order by transaction_date
    ) as diff,
    grouping.class,
    grouping.type,
    grouping.region
from {{ ref("stg_traderepublic") }} depot
left join {{ ref("traderepublic_grouping") }} grouping on depot.isin = grouping.isin
