with
    src as (
        select
            *,
            row_number() over (
                partition by referenznummer order by load_time desc
            ) as rn
        from {{ source("public", "barclays") }}
    )

select
    referenznummer as reference,
    buchungsdatum1 as transaction_date,
    -- convert to date
    to_date(buchungsdatum2, 'DD.MM.YYYY') as value_date,
    betrag as amount_org,
    cast(
        replace(
            replace(replace(replace(betrag, '€', ''), '+', ''), '.', ''), ',', '.'
        ) as float
    ) as amount_eur,
    betrag as amount,
    beschreibung as description,
    typ as type,
    status,
    kartennummer as card_number,
    originalbetrag as original_amount,
    mögliche_zahlpläne as possible_payment_plans,
    land as country,
    name_des_karteninhabers as cardholder_name,
    kartennetzwerk as card_network,
    kontaktlose_bezahlung as contactless_payment,
    load_time
from src
where rn = 1
