SELECT id AS subscription_id, MIN(occurred_at) first_churned_at
FROM finance.lifecycle
WHERE type = 'churned'
GROUP BY id