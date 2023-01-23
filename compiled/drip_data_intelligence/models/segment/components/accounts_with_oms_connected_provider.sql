WITH oms_provider_integrations AS (
  SELECT
    acc.id AS drip_account_id,
    inte.account_param,
    inte.created_at,
    CASE
      WHEN provider_param = 'drip_woocommerce' THEN 'woocommerce'
      ELSE provider_param
    END AS drip_oms_connected_provider
  FROM
    current.integrations AS inte
    JOIN "dumps"."dev"."accounts" AS acc
    ON inte.account_param = acc.public_id
  WHERE
    1 = 1
    AND provider_param IN (
      'magento',
      'bigcommerce',
      'drip_woocommerce',
      'shopify'
    )
    AND inte.deleted_at IS NULL
),
oms_provider_integrations_ranked AS (
  SELECT
    drip_account_id,
    account_param,
    drip_oms_connected_provider,
    created_at,
    DENSE_RANK() OVER (
      PARTITION BY drip_account_id
      ORDER BY
        created_at DESC
    ) AS drip_oms_connected_provider_rank
  FROM
    oms_provider_integrations
)
SELECT
  drip_account_id,
  drip_oms_connected_provider
FROM
  oms_provider_integrations_ranked
WHERE
  drip_oms_connected_provider_rank = 1