SELECT
    subscription_id AS drip_subscription_id,
    MAX(created_at) :: DATE AS drip_last_payment_date
FROM
    "dumps"."dev"."transactions"
WHERE
    TRUE
    AND status = 'paid'
GROUP BY
    drip_subscription_id