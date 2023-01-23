SELECT
  a.subscription_id AS drip_subscription_id,
  count(*) AS drip_sku_count
FROM
  current.products p
  JOIN "dumps"."dev"."accounts" a
  ON p.account_id = a.public_id
GROUP BY
  a.subscription_id