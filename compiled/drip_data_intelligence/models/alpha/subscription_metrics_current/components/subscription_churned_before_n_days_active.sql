

WITH churn_events AS (SELECT id AS subscription_id, MIN(occurred_at) first_churned_at
                      FROM finance.lifecycle
                      WHERE type = 'churned'
                      GROUP BY id),
     activated_events AS (SELECT id AS subscription_id, MIN(occurred_at) activated_at
                          FROM finance.lifecycle
                          WHERE type = 'activated'
                          GROUP BY id)
SELECT 
         DATE_DIFF('days', activated_at, first_churned_at) < 32 AS churned_before_32_days_active,
       
         DATE_DIFF('days', activated_at, first_churned_at) < 63 AS churned_before_63_days_active,
       
         DATE_DIFF('days', activated_at, first_churned_at) < 93 AS churned_before_93_days_active,
       
       subscription_id
FROM activated_events
         JOIN churn_events USING (subscription_id)