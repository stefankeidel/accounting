with
    old_tx as (
        select
            csvcolumn_date::date as transaction_date,
            csvcolumn_type as transaction_type,
            cast(
                regexp_replace(csvcolumn_value, '[^0-9.-]', '', 'g') as numeric
            ) as amount_eur,
            csvcolumn_note as note,
            load_time,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "csvcolumn_date",
                        "csvcolumn_type",
                        "csvcolumn_value",
                        "csvcolumn_note",
                    ]
                )
            }} as transaction_id
        from {{ source("public", "traderepublic_transactions") }}
    ),
    new_tx as (
        select
            date::date as transaction_date,
            type as transaction_type,
            cast(
                regexp_replace(value, '[^0-9.-]', '', 'g') as numeric
            ) as amount_eur,
            note as note,
            load_time,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "date",
                        "type",
                        "value",
                        "note",
                    ]
                )
            }} as transaction_id
        from {{ source("public", "traderepublic_transactions_v2") }}
    ),
    all_tx as (
        select * from old_tx
        union all
        select * from new_tx
    ),
    numbered as (
        select
            *,
            row_number() over (
                partition by transaction_id order by load_time desc
            ) as rn
        from all_tx
    )

select *
from numbered
where rn = 1
