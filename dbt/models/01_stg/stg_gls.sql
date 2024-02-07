with src_key as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key(['buchungstag', 'valutadatum', 'name_zahlungsbeteiligter', 'buchungstext', 'betrag', 'saldo_nach_buchung']) }} as transaction_id
    from {{ source('public', 'gls') }}
)

, src as (
    select
        *
        , to_date(valutadatum, 'DD.MM.YYYY') as value_date
        , cast(replace(betrag, ',', '.') as float) as amount_eur
        , row_number() over (partition by transaction_id order by load_time desc) as rn
    from src_key
)


select *
from src
where rn = 1
