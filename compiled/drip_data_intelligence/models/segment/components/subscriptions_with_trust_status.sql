SELECT
    user_id AS drip_user_id,
    trust_status AS drip_trust_status
FROM
    "dumps"."dev"."subscriptions"
WHERE
    1 = 1