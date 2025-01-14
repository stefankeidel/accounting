select
  *
  , 'Assets:Investments:TradeRepublic' as account
  , -- difference to previous row for transaction ledger
  amount_eur - lag(amount_eur, 1, 0) over (partition by isin order by transaction_date) as diff
from {{ ref('stg_traderepublic') }}
