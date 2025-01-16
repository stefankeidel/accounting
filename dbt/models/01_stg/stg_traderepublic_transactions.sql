with tx as (

select
    csvcolumn_date::date as transaction_date
    , csvcolumn_type as transaction_type
    , CAST(REGEXP_REPLACE(csvcolumn_value, '[^0-9.-]', '', 'g') AS NUMERIC) as amount_eur
    , csvcolumn_note as note
    , load_time
    , {{ dbt_utils.generate_surrogate_key(['csvcolumn_date', 'csvcolumn_type', 'csvcolumn_value', 'csvcolumn_note']) }} as transaction_id
from {{ source('public', 'traderepublic_transactions') }}
)
, numbered as (
    select 
        *
        , row_number() over (partition by transaction_id order by load_time desc) as rn
    from tx
)

select *
from numbered
where rn = 1