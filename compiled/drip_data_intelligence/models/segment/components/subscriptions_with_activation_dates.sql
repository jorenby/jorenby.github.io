SELECT
    subscription_id AS drip_subscription_id,
    activated_at :: DATE AS drip_activation_date,
    activated_week_cohort AS drip_activation_date_cohort_week
FROM
    "dumps"."current"."subscription_details"