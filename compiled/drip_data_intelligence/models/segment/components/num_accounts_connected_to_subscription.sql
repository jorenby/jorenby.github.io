SELECT
    subscription_id AS drip_subscription_id,
    count(id) AS drip_accounts_connected_subscription
FROM
    "dumps"."dev"."accounts"
GROUP BY
    drip_subscription_id