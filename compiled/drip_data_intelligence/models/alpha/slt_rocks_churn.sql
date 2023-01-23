SELECT *,
       CASE
           WHEN mrr_before_churn <= 250 THEN 'VSB'
           WHEN mrr_before_churn <= 1200 THEN 'SMB'
           ELSE 'MM'
           END                         AS subscription_size,
       occurred_date - activation_date AS lifespan,
       mrr_before_churn * 12           AS arr_before_churn,
       CASE --JN wants 3 buckets [first 90 days, first year, 1 year +]
           WHEN lifespan < 91 THEN 'FIRST 90 DAYS'
           WHEN lifespan < 366 THEN 'FIRST YEAR'
           ELSE 'OVER 1 YEAR'
           END                         AS lifespan_bucket,
       CASE
           WHEN occurred_date BETWEEN CAST(CAST(getdate() AS DATE) - 7 AS DATE) AND CAST(CAST(getdate() AS DATE) - 1 AS DATE)
               THEN TRUE
           ELSE FALSE END              AS trailing_7_days,
       CASE
           WHEN occurred_date BETWEEN CAST(CAST(getdate() AS DATE) - 30 AS DATE) AND CAST(CAST(getdate() AS DATE) - 1 AS DATE)
               THEN TRUE
           ELSE FALSE END              AS trailing_30_days,
       CASE
           WHEN occurred_date BETWEEN CAST(CAST(getdate() AS DATE) - 90 AS DATE) AND CAST(CAST(getdate() AS DATE) - 1 AS DATE)
               THEN TRUE
           ELSE FALSE END              AS trailing_90_days
FROM (
         SELECT subscription_id, CAST(activated_at AS DATE) activation_date, indicated_an_oms
         FROM "dumps"."dev"."subscription_metrics_current"
         WHERE current_mrr = 0
         --assuming all churn subscriptions have current_mrr == 0
     )
         LEFT JOIN (SELECT DISTINCT id                        as subscription_id,
                                    CAST(occurred_at AS DATE) AS occurred_date,
                                    LAST_VALUE(previous_value)
                                    over (PARTITION BY id ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) /
                                    100                       AS mrr_before_churn
                                    --grabbing the previous mrr right before churn
                    FROM finance.mrr_changed)
                   USING (subscription_id)

WHERE occurred_date >= '2021-01-01'
ORDER BY occurred_date DESC