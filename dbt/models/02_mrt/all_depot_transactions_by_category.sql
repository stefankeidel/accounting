with
    all_asset_tx as (
        select
            load_time as snapshot_date,
            wkn as id,
            'gls' as source,
            class,
            type,
            region,
            price_eur
        from {{ ref("gls_depot_assets") }}
        where class is not null
    ),
    with_diff as (
        select
            snapshot_date,
            id,
            source,
            class,
            type,
            region,
            price_eur,
            price_eur - lag(price_eur, 1, 0) over (
                partition by id, class, type, region order by snapshot_date
            ) as diff_price_eur
        from all_asset_tx
    ),
    agg as (
        select
            date_trunc('month', snapshot_date) as snapshot_month,
            id,
            source,
            class,
            type,
            region,
            sum(diff_price_eur) as diff_price_month
        from with_diff {{ dbt_utils.group_by(n=6) }}
    )
select
    *,
    sum(diff_price_month) over (
        partition by id, class, type, region order by snapshot_month
    ) as diff_rolling
from agg
