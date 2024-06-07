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

        when description in ('Bike24', 'Bike-Discount') then 'Expenses:Bike'
        when description ilike '%r2 Handels GmbH%' then 'Expenses:Bike'
        when description ilike '%H&S BIKE-DISCOUNT%' then 'Expenses:Bike'
        when description ilike '%Internetstores%' then 'Expenses:Bike'
        when description ilike '%Komponentix%' then 'Expenses:Bike'
        when description ilike '%BIKECOMPON%' then 'Expenses:Bike'
        when description ilike '%cotic%' then 'Expenses:Bike'
        when description ilike '%decathlon%' then 'Expenses:Bike'
        when description ilike '%Globetrotter%' then 'Expenses:Bike'
        when description ilike '%BERGFREUNDE%' then 'Expenses:Bike'
        when description ilike '%TAILFIN%' then 'Expenses:Bike'


        when description in ('REWE', 'Penny Market', 'EDEKA') then 'Expenses:Mandatory'
        when description like '%REWE%' then 'Expenses:Mandatory'
        when description ilike '%edeka%' then 'Expenses:Mandatory'
        when description ilike '%HIT ECHTE VIELFALT%' then 'Expenses:Mandatory'
        when description ilike '%nur hier gmbh%' then 'Expenses:Mandatory'
        when description ilike '%aldi nord%' then 'Expenses:Discretionary'
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
        when description ilike '%NextDNS%' then 'Expenses:Subscriptions'
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
        when description ilike '%bvg%' then 'Expenses:Transportation'
        when description ilike '%NAH.SH%' then 'Expenses:Transportation'
        when description ilike '%DB Vertrieb%' then 'Expenses:Transportation'
        when description ilike '%Rczbikeshop%' then 'Expenses:Discretionary'
        when description ilike '%DHL%' then 'Expenses:Discretionary'
        when description ilike '%DPD%' then 'Expenses:Discretionary'
        when description ilike '%HERMES%' then 'Expenses:Discretionary'
        when description ilike 'EUROWINGS%' then 'Expenses:Travel'
        when description ilike 'LUFTHANSA%' then 'Expenses:Travel'
        when description ilike '%Kobe Nord%' then 'Expenses:Discretionary'
        when description ilike '%Sudden Death%' then 'Expenses:Discretionary'
        when description ilike '%Cafe Luise%' then 'Expenses:Discretionary'
        when description ilike '%IKEA%' then 'Expenses:Discretionary'
        when description ilike '%Telekom Deutschland GmbH%' then 'Expenses:Subscriptions'
        when description ilike '%Deutsche Bahn%' then 'Expenses:Transportation'
        when description ilike '%DB HAMBURG%' then 'Expenses:Transportation'
        when description ilike '%DB Fern%' then 'Expenses:Transportation'
        when description ilike '%Aral%' and amount_eur > -30 then 'Expenses:Discretionary'

        when description ilike '%Brauhaus%' then 'Expenses:Discretionary'
        when description ilike '%SATURN%' then 'Expenses:Discretionary'
        when description ilike '%BUDNI%' then 'Expenses:Mandatory'
        when description ilike '%Eat better now%' then 'Expenses:Discretionary'
        when description ilike '%A-ROSA%' then 'Expenses:Travel'
        when description ilike '%FPS Justice-Cross Bord Brussels%' then 'Expenses:Transportation'
        when description ilike '%Superfreunde%' then 'Expenses:Discretionary'
        when description ilike '%Miete Justus-Strandes-Weg%' then 'Expenses:Rent'
        when description ilike '%Dominos%' then 'Expenses:Discretionary'
        when description ilike '%Hetzner%' then 'Expenses:Subscriptions'
        when description ilike '%Sanikonzept%' then 'Expenses:Discretionary'

        when description ilike '%FRIDAY Insurance%' then 'Expenses:Insurance'
        when description ilike '%WERTPAPIERABRECHNUNG%' then 'Expenses:Investments'
        when description ilike '%COSMOS Lebensversicherungs-Aktiengesellschaft%' then 'Expenses:Investments'
        when description ilike '%WP-ERTRÄGNISGUTSCHRIFT%' then 'Income:Dividends'
        when description ilike '%SCHECK-NR.%' then 'Income:Refunds'

        when description ilike '%Vodafone%' then 'Expenses:Eitelstr'
        when description ilike '%Helvetica Services%' then 'Expenses:Eitelstr'
        when description ilike '%Giorgi%' then 'Expenses:Eitelstr'
        when description ilike '%Lichtblick SE%' and amount_eur < -60 then 'Expenses:Eitelstr'
        when description ilike '%Lichtblick SE%' and amount_eur < 0 then 'Expenses:Utilities'
        when description ilike '%Lichtblick SE%' and amount_eur > 0 then 'Income:Salary'

        when description ilike '%DEPOTENTGELT%' then 'Expenses:Subscriptions'
        when description ilike '%Abschluss per %' then 'Expenses:Subscriptions'
        when description ilike '%GLS Beitrag%' then 'Expenses:Subscriptions'

        when description ilike '%Lea%' then 'Income:Selling Used Shit'
        when description ilike '%vaude regen%' then 'Income:Selling Used Shit'
        when description ilike '%Nils Thomsen%' then 'Expenses:Discretionary'
        when description ilike '%Booking.com%' then 'Expenses:Travel'
        when description ilike '%E NEUKAUF%' then 'Expenses:Discretionary'
        when description ilike '%KINOHELD.DE%' then 'Expenses:Discretionary'
        when description ilike '%ZWEIRADHOTEL%' then 'Expenses:Travel'
        when description ilike '%Schnitzelhaus%' then 'Expenses:Discretionary'
        when description ilike '%FAMILIENBAECKEREI KOLL QUICKBORN%' then 'Expenses:Discretionary'
        when description ilike '%Domino%Pizza%' then 'Expenses:Discretionary'
        when description ilike '%Pizzeria%' then 'Expenses:Discretionary'
        when description ilike '%BILLY THE BUTCHER%' then 'Expenses:Discretionary'
        when description ilike '%KAUFHAUS HANS J. LEMBE%' then 'Expenses:Discretionary'
        when description ilike '%Alte Muehle Sankt Engl%' then 'Expenses:Discretionary'
        when description ilike '%Herzi%Wirtschaft%' then 'Expenses:Discretionary'
        when description ilike '%Hoflinger%' then 'Expenses:Discretionary'
        when description ilike '%LEUCHTFEUER%' then 'Expenses:Discretionary'

        when description ilike '%NAME-CHEAP%' then 'Expenses:Subscriptions'
        when description ilike '%Taxfix%' then 'Expenses:Subscriptions'
        when description ilike '%Rundfunk ARD, ZDF, DRadio%' then 'Expenses:Utilities'
        when description ilike '%DAILY BREAD%' then 'Expenses:Bike'
        when description ilike '%TRANGIA%' then 'Expenses:Bike'

        when description ilike '%CINEMAXX%' then 'Expenses:Discretionary'
        when description ilike '%DALLMEYERS%' then 'Expenses:Mandatory'
        when description ilike '%AVIA%' then 'Expenses:Discretionary'
        when description ilike '%Regiomat%' then 'Expenses:Discretionary'
        when description ilike '%Airbnb%' then 'Expenses:Travel'
        when description ilike '%Scotrail%' then 'Expenses:Travel'
        when description ilike '%Hotels%' then 'Expenses:Travel'
        when description ilike '%Pension%' then 'Expenses:Travel'
        when description ilike '%Mietwagen%' then 'Expenses:Travel'
        when description ilike '%Smartments%' then 'Expenses:Travel'

        when description ilike '%RAIFFEISENBANK CHAMER LAND%' then 'Expenses:Cash'
        when description ilike '%VR GENOBANK DONAUWALD%' then 'Expenses:Cash'

        when description ilike '%HOPS%BARLEY%' then 'Expenses:Discretionary'
        when description ilike '%BraufactuM%' then 'Expenses:Discretionary'
        when description ilike '%Veloheld%' then 'Expenses:Bike'
        when description ilike '%STEINECKES%' then 'Expenses:Mandatory'
        when description ilike '%BERNAUER BRAUG%' then 'Expenses:Discretionary'
        when description ilike '%FIVE ELEPHANT%' then 'Expenses:Discretionary'
        when description ilike '%SCHAEFERS%' then 'Expenses:Discretionary'
        when description ilike '%Alnatura%' then 'Expenses:Discretionary'

        when description ilike '%Per Lastschrift dankend erhalten%' then 'Transfer'
        when description ilike '%BARCLAYS LASTSCHRIFT%' then 'Transfer'

        when description ilike 'baeckerei%' then 'Expenses:Mandatory'
        when description ilike '%ALLWÖRDEN%' then 'Expenses:Mandatory'
        when description ilike 'AMZN%' then 'Expenses:Discretionary'
        when description ilike '%SUPERMARKT%' then 'Expenses:Mandatory'
        when description ilike 'paypal%' then 'Expenses:Discretionary'

        when description ilike '%Ausgleich April 2024' then 'Income:Refunds'

        when description = 'Some CSV export from depot' then 'Income:Capital Gains'
        when description = 'Fake depot to 0 transaction'  then 'Income:Capital Gains'

        else null
      end as category
from to_map
