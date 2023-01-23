WITH accounts_with_oms_integration_events AS (
    SELECT
        distinct acc.id
        , 1 AS has_oms_integration_events
    FROM current.integrations AS inte
    JOIN "dumps"."dev"."accounts" acc
        ON inte.account_param = acc.public_id
    WHERE
        1 = 1
    AND provider_param IN (
        'magento',
        'bigcommerce',
        'drip_woocommerce',
        'shopify'
    )
)
SELECT
    acc.id AS drip_account_id
    , CASE WHEN has_oms_integration_events = 1 THEN TRUE ELSE FALSE END AS drip_has_ever_connected_oms
FROM "dumps"."dev"."accounts" acc
LEFT JOIN accounts_with_oms_integration_events USING (id)