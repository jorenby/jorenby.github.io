WITH purchases_currency_codes_converted AS (
      SELECT id
           , account_id
           , subscriber_id
           , provider
           , order_id
           , CASE WHEN currency_code = 'IRR' THEN amount / 100 ELSE amount END AS amount
           , permalink
           , created_at
           , updated_at
           , public_id
           , occurred_at
           , upstream_id
           , identifier
           , financial_state
           , fulfillment_state
           , tax
           , fees
           , discount
           , closed_at
           , billing_address_id
           , shipping_address_id
           , upstream_updated_at
           , CASE
            WHEN currency_code = '₫' THEN 'VIE'
            WHEN currency_code = 'DKR' THEN 'DKK'
            WHEN currency_code = 'USD,USD' THEN 'USD'
            WHEN currency_code = 'IRT' THEN 'IRR'
            WHEN currency_code = 'USD,USD,USD' THEN 'USD'
            WHEN currency_code = 'CA' THEN 'USD'
            WHEN currency_code = 'AL' THEN 'USD'
            WHEN currency_code = 'US$' THEN 'USD'
            WHEN currency_code = 'CO' THEN 'USD'
            WHEN currency_code = 'MS' THEN 'USD'
            WHEN currency_code = 'IE-EUR' THEN 'EUR'
            WHEN currency_code = 'USD,USD,USD,USD' THEN 'USD'
            WHEN currency_code = 'OR' THEN 'USD'
            WHEN currency_code = 'WI' THEN 'USD'
            WHEN currency_code = 'NE' THEN 'USD'
            WHEN currency_code = 'ID' THEN 'USD'
            WHEN currency_code = 'OK' THEN 'USD'
            WHEN currency_code = 'LA' THEN 'USD'
            WHEN currency_code = 'UT' THEN 'USD'
            WHEN currency_code = 'ME' THEN 'USD'
            WHEN currency_code = 'KS' THEN 'USD'
            WHEN currency_code = 'IN' THEN 'USD'
            WHEN currency_code = 'KY' THEN 'USD'
            WHEN currency_code = 'AK' THEN 'USD'
            WHEN currency_code = 'OH' THEN 'USD'
            WHEN currency_code = 'CT' THEN 'USD'
            WHEN currency_code = 'VT' THEN 'USD'
            WHEN currency_code = 'IA' THEN 'USD'
            WHEN currency_code = 'WA' THEN 'USD'
            WHEN currency_code = 'MI' THEN 'USD'
            WHEN currency_code = 'MD' THEN 'USD'
            WHEN currency_code = 'TN' THEN 'USD'
            WHEN currency_code = 'ND' THEN 'USD'
            WHEN currency_code = 'AR' THEN 'USD'
            WHEN currency_code = 'ÎGBP' THEN 'GBP'
            WHEN currency_code = 'SKR' THEN 'SEK'
            WHEN currency_code = 'USD,USD,USD,USD,USD' THEN 'USD'
            ELSE UPPER(currency_code)
            END AS currency_code
            , shipping
            , canceled_at
            , attributed_delivery_id
            , attributed_emailable_id
            , attributed_emailable_type
            , attributed_automation_id
            , attributed_automation_type
            , attributed_email_id
            , source
            , attributed_smsable_id
            , attributed_smsable_type
            , attributed_sms_delivery_id
        FROM "dumps"."dev"."purchases"
  )
SELECT purchases_currency_codes_converted.*
     , nvl(CASE
           WHEN currency_code != 'USD'
               THEN amount::real / currency_value_to_usd
               ELSE amount END / 100 ,0)   AS amount_usd
     , nvl(CASE
           WHEN currency_code != 'USD'
               THEN (discount::real / currency_value_to_usd)
               ELSE discount END / 100 ,0) AS discount_usd
     , nvl(CASE
           WHEN currency_code != 'USD'
               THEN (shipping::real / currency_value_to_usd)
               ELSE shipping END / 100 ,0) AS shipping_usd
     , nvl(CASE
           WHEN currency_code != 'USD'
               THEN (tax::real / currency_value_to_usd)
               ELSE tax END / 100 ,0)      AS tax_usd
     , amount_usd - shipping_usd - tax_usd AS revenue_amount_usd
  FROM purchases_currency_codes_converted
           LEFT JOIN "dumps"."alpha"."historical_currency_conversion"
                     ON currency_code = purchase_currency
                         AND historical_currency_conversion.occurred_at
                            = date_trunc('d', purchases_currency_codes_converted.occurred_at)