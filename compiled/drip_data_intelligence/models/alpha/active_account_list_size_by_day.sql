WITH calendar AS (
  SELECT
    *
  FROM "dumps"."dev"."backfill_calendar"
  WHERE TRUE
    AND backfill_calendar.as_of_date >= SYSDATE - 730
), active_subscriptions AS (
  --the aggregation is a little different here, I use windows to find the most recent event and put that on every day's record, this works much faster than I'd assumed it would
  SELECT
    subscription_id
    , account_id
    , account_public_id
    , as_of_date
    , activation_date
    , dim_date.first_date_of_week activation_cohort_week
    , mrr_current
    , is_active
    , active_and_churn_day_row_filter
  FROM (
    SELECT
      subscription_id
      , account_id
      , account_public_id
      , as_of_date
      , MIN(events_date) OVER (PARTITION BY subscription_id) AS activation_date
      , mrr_cents_current / 100 AS mrr_current
      , CASE WHEN mrr_cents_current > 0 THEN TRUE ELSE FALSE END AS is_active
      , CASE WHEN (mrr_cents_current = 0 AND as_of_date = events_date) OR mrr_cents_current > 0 THEN TRUE END AS active_and_churn_day_row_filter
      , ROW_NUMBER() OVER (partition by subscription_id, as_of_date ORDER BY events_date DESC) AS n
    FROM calendar
    JOIN "dumps"."dev"."customer_mrr_changed"
        ON events_date = DATE_TRUNC('D', occurred_at)
    JOIN (
      SELECT
        DISTINCT subscription_id
        , account_id
        , account_public_id
      FROM "dumps"."dev"."account_metrics_current") AS account_grain
      USING (subscription_id)
    ) AS prep_subscriptions
    JOIN "dumps"."alpha"."dim_date"
      ON calendar_date = activation_date
    WHERE TRUE
      AND n = 1
      AND is_active
), list_size AS (
  SELECT
    account_daily_usage_stats.account_id
    , occurred_on
    , billable_subscribers_v2
  FROM current.account_daily_usage_stats
  LEFT JOIN active_subscriptions
    ON active_subscriptions.account_id = account_daily_usage_stats.account_id
    AND occurred_on::date = as_of_date
  WHERE TRUE
    AND active_subscriptions.is_active
)
SELECT
  account_id
  , occurred_on::DATE AS occurred_on
  , SUM(billable_subscribers_v2) AS billable_subscribers
  , billable_subscribers - LAG(billable_subscribers) OVER (PARTITION BY account_id ORDER BY occurred_on) AS change_in_billable_subscribers_1_day
FROM list_size
GROUP BY occurred_on, account_id