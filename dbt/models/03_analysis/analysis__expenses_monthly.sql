with
    filtered as (
        select * from {{ ref("analysis__transactions_with_categories") }}
    -- this is just to potentially filter this maybe soon™
    ),
    pivot as (
        select
            category,
            {{
                dbt_utils.pivot(
                    "month",
                    dbt_utils.get_column_values(
                        ref("analysis__transactions_with_categories"),
                        "month",
                        order_by="month desc",
                    ),
                    agg="sum",
                    then_value="amount_eur",
                )
            }}
        from filtered
        group by 1
    )
select *
from pivot

order by 2 asc
