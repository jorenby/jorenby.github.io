SELECT subject_id AS subscription_id, MIN(created_at) AS trial_expired_at
FROM data_intelligence.subscription_state_transitions
WHERE "to" = 'trial_expired'
GROUP BY subject_id