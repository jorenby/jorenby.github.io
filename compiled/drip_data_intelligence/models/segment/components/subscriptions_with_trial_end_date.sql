SELECT
    subject_id as drip_subscription_id,
    MIN(created_at) as drip_trial_end_date
FROM
    data_intelligence.subscription_state_transitions
WHERE
    1 = 1
    AND "to" = 'trial_expired'
GROUP BY
    drip_subscription_id