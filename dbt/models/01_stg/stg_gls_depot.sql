select
    name as product_name,
    upper(trim(wkn)) as wkn,
    replace(
        replace(replace("stück/nominal", ' St.', ''), ' Stk.', ''), ',', '.'
    )::decimal(10, 3) as quantity,
    replace(replace(replace(aktueller_kurs, ' EUR', ''), '.', ''), ',', '.')::decimal(
        10, 2
    ) as piece_eur,
    replace(replace(replace(kurswert_in_eur, ' EUR', ''), '.', ''), ',', '.')::decimal(
        10, 2
    ) as price_eur,
    replace(
        replace(replace("kursgewinn/-verlust_in_eur", ' EUR', ''), '.', ''), ',', '.'
    )::decimal(10, 2) as delta_eur,
    load_time
from {{ source("public", "gls_depot") }}
