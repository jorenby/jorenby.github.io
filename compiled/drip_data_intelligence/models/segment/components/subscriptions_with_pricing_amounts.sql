SELECT
    id AS drip_subscription_id,
    ROUND(amount :: NUMERIC / 100, 2) AS drip_pricing_amount
FROM
    "dumps"."dev"."subscriptions"