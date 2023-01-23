SELECT
    subscription_id AS drip_subscription_id,
    trial_week_cohort AS drip_trial_cohort_week
FROM
    "dumps"."current"."subscription_details"