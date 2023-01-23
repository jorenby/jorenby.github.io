WITH metric_rows AS (
  SELECT
    account_id,
    billable_subscribers_v2,
    ROW_NUMBER() OVER (
      PARTITION BY account_id
      ORDER BY
        occurred_on DESC
    ) AS row_number
  FROM
    "dumps"."current"."account_daily_usage_stats"
),
most_recent_metrics AS (
  SELECT
    account_id,
    billable_subscribers_v2
  FROM
    metric_rows
  WHERE
    row_number = 1
),
most_recent_metrics_with_subscription AS (
  SELECT
    *,
    SUM(billable_subscribers_v2) OVER (PARTITION BY subscription_id) AS total_subscribers_in_subscription,
    billable_subscribers_v2 :: FLOAT / NULLIF(total_subscribers_in_subscription, 0) :: FLOAT AS weight_by_pct_subscribers
  FROM
    most_recent_metrics
    JOIN (
      SELECT
        id AS account_id,
        subscription_id
      FROM
        "dumps"."dev"."accounts"
    ) USING (account_id)
),
account_count_subquery AS (
  SELECT
    subscription_id,
    COUNT(*) AS n_accounts
  FROM
    "dumps"."dev"."accounts"
  GROUP BY
    subscription_id
)
SELECT
  account_id,
  pct_account_subscribers * current_mrr AS estimated_mrr
FROM
  (
    SELECT
      account_id,
      subscription_id,
      CASE
        WHEN total_subscribers_in_subscription > 0 THEN weight_by_pct_subscribers
        ELSE 1 / CAST(n_accounts AS FLOAT)
      END AS pct_account_subscribers
    FROM
      most_recent_metrics_with_subscription
      JOIN (
        SELECT
          *
        FROM
          account_count_subquery
      ) USING (subscription_id)
  )
  JOIN (
    SELECT
      subscription_id,
      CASE
        WHEN current_mrr IS NULL THEN 0
        ELSE current_mrr
      END AS current_mrr
    FROM
      "dumps"."dev"."subscription_metrics_current"
  ) USING (subscription_id)