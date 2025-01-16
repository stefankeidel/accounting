with

    valutadatum_replaced as (
        select replace(valutadatum, '30.02.', '29.02.') as valutadatum_replaced, *
        from {{ source("public", "gls") }}
    ),
    src_key as (
        select
            *,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "valutadatum_replaced",
                        "name_zahlungsbeteiligter",
                        "buchungstext",
                        "betrag",
                        "saldo_nach_buchung",
                    ]
                )
            }} as transaction_id
        from valutadatum_replaced
    ),
    src as (
        select
            *,
            to_date(valutadatum_replaced, 'DD.MM.YYYY') as value_date,
            cast(replace(betrag, ',', '.') as float) as amount_eur,
            row_number() over (
                partition by transaction_id order by load_time desc
            ) as rn
        from src_key
    )

select *
from src
where
    rn = 1
    -- it's called Data Engineering, bro! Found a transaction where the
    -- buchungstext changed between two imports of the same thing. messed up
    -- everything, lol
    and transaction_id not in ('6e43801c2628fe73afdf8881f0280b76')
