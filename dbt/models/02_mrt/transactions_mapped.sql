{{ config(disabled=true) }}

with to_map as (

    select
        transaction_id
        , source
        , transaction_date
        , description
        , account
        , amount_eur
    from {{ ref('transactions') }}
    where category is null or category = 'Expenses:TODO'

)

select
    transaction_id
    , source
    , transaction_date
    , description
    , account
    , amount_eur
    , case
        when description = 'Belastung Jahresbeitrag' then 'Expenses:Subscriptions'
        when lower(description) = 'paypal' and amount_eur < 0 then 'Expenses:Discretionary' -- not entirely true but meh. Trying to get away from PP anyway
        when description = 'PayPal' and amount_eur > 0 then 'Expenses:Discretionary' -- this means Kleinanzeigen sales go into that bucket, but also meh
        when description like '%Hamburg Service%' then 'Expenses:Mandatory'
        when description in ('Bike24', 'Bike-Discount') then 'Expenses:Discretionary'
        when description in ('REWE', 'Penny Market', 'EDEKA') then 'Expenses:Mandatory'
        when description ilike '%edeka%' then 'Expenses:Mandatory'
        when description ilike '%nur hier gmbh%' then 'Expenses:Mandatory'
        when description ilike '%aldi nord%' then 'Expenses:Discretionary'
        when description ilike '%cotic%' then 'Expenses:Discretionary'
        when description ilike '%BIKECOMPON%' then 'Expenses:Discretionary'
        when description ilike '%flaschenp%' then 'Expenses:Discretionary'
        when description ilike '%Apple Store%' then 'Expenses:Discretionary'
        when description ilike '%Apple%' then 'Expenses:Subscriptions'
        when description ilike '%Too Good To Go%' then 'Expenses:Mandatory'
        when description ilike '%Ryanair%' then 'Expenses:Travel'
        when description ilike '%EXTREMTEXTI%' then 'Expenses:Discretionary'
        when description ilike '%BRAUSTURM%' then 'Expenses:Discretionary'
        when description ilike '%KAFFEEREI%' then 'Expenses:Discretionary'
        when description ilike '%Amazon Marketplace%' then 'Expenses:Discretionary'
        when description ilike '%Amazon%' then 'Expenses:Discretionary'
        when description ilike '%DigitalOcean%' then 'Expenses:Subscriptions'
        when description ilike '%Rossmann%' then 'Expenses:Mandatory'
        when description ilike '%Konditorei Junge%' then 'Expenses:Mandatory'
        when description ilike '%Telefonica%' then 'Expenses:Subscriptions'
        when description ilike '%Penny%' then 'Expenses:Mandatory'
        when description ilike '%LACHM%' then 'Expenses:Subscriptions'
        when description ilike '%Lichtblick%' and amount_eur > 1000 then 'Income:Salary'
        when description ilike '%elbgold%' then 'Expenses:Discretionary'
        when description ilike '%BEETS%' then 'Expenses:Discretionary'
        when description ilike '%Openai%' then 'Expenses:Subscriptions'
        when description ilike '%BRAUGASTHAUS%' then 'Expenses:Discretionary'
        when description ilike '%VON ALLWOERD%' then 'Expenses:Mandatory'
        when description ilike '%McDonald%' then 'Expenses:Discretionary'
        when description ilike '%Aena%' then 'Expenses:Discretionary'
        when description ilike '%MORA VICENS SL%' then 'Expenses:Discretionary'
        when description ilike '%Lidl%' then 'Expenses:Mandatory'
        when description ilike 'REWE%' then 'Expenses:Mandatory'
        when description ilike 'nur hier%' then 'Expenses:Mandatory'
        when description ilike '%decathlon%' then 'Expenses:Mandatory'
        when description ilike '%bvg%' then 'Expenses:Transportation'
        when description ilike '%NAH.SH%' then 'Expenses:Transportation'
        when description ilike '%DB Vertrieb%' then 'Expenses:Transportation'
        when description ilike '%Rczbikeshop%' then 'Expenses:Discretionary'
        when description ilike '%DHL%' then 'Expenses:Discretionary'
        when description ilike '%HERMES%' then 'Expenses:Discretionary'
        when description ilike 'EUROWINGS%' then 'Expenses:Travel'
        when description ilike 'LUFTHANSA%' then 'Expenses:Travel'
        when description ilike '%Kobe Nord%' then 'Expenses:Discretionary'
        when description ilike '%Sudden Death%' then 'Expenses:Discretionary'
        when description ilike '%Cafe Luise%' then 'Expenses:Discretionary'
        when description ilike '%IKEA%' then 'Expenses:Discretionary'
        

        when description ilike 'baeckerei%' then 'Expenses:Mandatory'
        when description ilike 'AMZN%' then 'Expenses:Discretionary'
        when description ilike 'paypal%' then 'Expenses:Discretionary'
        else null
      end as category
from to_map
