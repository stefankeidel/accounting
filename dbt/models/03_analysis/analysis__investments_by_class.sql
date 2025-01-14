with pivot as (
    select
        snapshot_date
        , {{ dbt_utils.pivot(
                 'class',
                 dbt_utils.get_column_values(ref('all_depot_transactions_by_category'), 'class'),
                 agg='sum',
                 then_value='total_value',
             )
          }}
    from {{ ref('all_depot_transactions_by_category') }}
    group by 1
)

select
    snapshot_date
    , equity
    , bond
    , equity+bond as total
    , 100.0 * equity / (equity+bond) as equity_percentage
    , 100.0 * bond / (equity+bond) as bond_percentage
    , equity - lag(equity, 1, equity) over (order by snapshot_date) as equity_diff
    , bond - lag(bond, 1, bond) over (order by snapshot_date) as bond_diff
    , equity+bond - lag(equity+bond, 1, equity+bond) over (order by snapshot_date) as total_diff
    , 100.0 * equity / (equity+bond) - lag(100.0 * equity / (equity+bond), 1, 100.0 * equity / (equity+bond)) over (order by snapshot_date) as equity_percentage_diff
    , 100.0 * bond / (equity+bond) - lag(100.0 * bond / (equity+bond), 1, 100.0 * bond / (equity+bond)) over (order by snapshot_date) as bond_percentage_diff
    , 100.0 * (equity+bond - lag(equity+bond) over (order by snapshot_date)) / lag(equity+bond) over (order by snapshot_date) as percentage_change
from pivot
