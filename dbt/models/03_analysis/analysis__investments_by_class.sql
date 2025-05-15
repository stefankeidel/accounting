with
    pivot as (
        select
            snapshot_month,
            {{
                dbt_utils.pivot(
                    "class",
                    dbt_utils.get_column_values(
                        ref("all_depot_transactions_by_category"), "class"
                    ),
                    agg="sum",
                    then_value="diff_rolling",
                )
            }}
        from {{ ref("all_depot_transactions_by_category") }}
        group by 1
    ),
    diffs as (
        select
            snapshot_month,
            equity,
            bond,
            equity + bond as total,
            100.0 * equity / (equity + bond) as equity_percentage,
            100.0 * bond / (equity + bond) as bond_percentage,
            equity
            - lag(equity, 1, equity) over (order by snapshot_month) as equity_diff,
            bond - lag(bond, 1, bond) over (order by snapshot_month) as bond_diff,
            equity
            + bond
            - lag(equity + bond, 1, equity + bond) over (
                order by snapshot_month
            ) as total_diff,
            100.0 * equity / (equity + bond) - lag(
                100.0 * equity / (equity + bond), 1, 100.0 * equity / (equity + bond)
            ) over (order by snapshot_month) as equity_percentage_diff,
            100.0 * bond / (equity + bond) - lag(
                100.0 * bond / (equity + bond), 1, 100.0 * bond / (equity + bond)
            ) over (order by snapshot_month) as bond_percentage_diff,
            100.0
            * (equity + bond - lag(equity + bond) over (order by snapshot_month))
            / lag(equity + bond) over (order by snapshot_month) as percentage_change
        from pivot
    )

select
    snapshot_month,
    round(cast(equity as numeric), 2) as equity,
    round(cast(bond as numeric), 2) as bond,
    round(cast(total as numeric), 2) as total,
    round(cast(equity_percentage as numeric), 2) as equity_percentage,
    round(cast(bond_percentage as numeric), 2) as bond_percentage,
    round(cast(equity_diff as numeric), 2) as equity_diff,
    round(cast(bond_diff as numeric), 2) as bond_diff,
    round(cast(total_diff as numeric), 2) as total_diff,
    round(cast(equity_percentage_diff as numeric), 2) as equity_percentage_diff,
    round(cast(bond_percentage_diff as numeric), 2) as bond_percentage_diff,
    round(cast(percentage_change as numeric), 2) as percentage_change
from diffs
