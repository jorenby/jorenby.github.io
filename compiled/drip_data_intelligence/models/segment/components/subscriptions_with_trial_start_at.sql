SELECT
    id AS drip_subscription_id,
    started_at AS drip_trial_start_date
FROM
    "dumps"."dev"."subscriptions"
WHERE
    1 = 1