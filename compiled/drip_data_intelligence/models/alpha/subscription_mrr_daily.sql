WITH raw_subscription_mrr AS (
  SELECT
    subscription_id,
    monthly_recurring_revenue_cents AS current_mrr_cents,
    TO_DATE(
      (
        year :: text || '-' || month :: text || '-' || day :: text
      ),
      'YYYY-MM-dd'
    ) AS mrr_date,
    MIN(mrr_date) OVER (PARTITION BY subscription_id) activation_date,
    category
  FROM
    "dumps"."current"."subscription_recurring_revenues"
  WHERE 1 = 1
    AND NOT test_subscription
),
calendar AS (
  SELECT DISTINCT subscription_id, calendar_date
    FROM "dumps"."dev"."invoices"
            JOIN "dumps"."dev"."invoice_items" ON invoices.id = invoice_id
            JOIN "dumps"."alpha"."dim_date" on calendar_date >= period_first_day and calendar_date <= period_last_day
  ORDER BY subscription_id, calendar_date
),
subscription_activations AS (
  SELECT
    subscription_id,
    MIN(mrr_date) activation_date,
    category
  FROM
    raw_subscription_mrr
  GROUP BY
    subscription_id,
    category
),
subscription_dates_since_activation AS (
  SELECT
    subscription_id,
    calendar_date as mrr_date,
    category
  FROM
    calendar
    join subscription_activations using (subscription_id)
)
SELECT
  COALESCE(
    raw_subscription_mrr.subscription_id,
    subscription_dates_since_activation.subscription_id
  ) AS subscription_id,
  COALESCE(current_mrr_cents, 0) AS current_mrr_cents,
  COALESCE(
    raw_subscription_mrr.mrr_date,
    subscription_dates_since_activation.mrr_date
  ) AS mrr_date,
  COALESCE(
    raw_subscription_mrr.category,
    subscription_dates_since_activation.category
  ) AS category
FROM
  subscription_dates_since_activation
  LEFT JOIN raw_subscription_mrr USING (subscription_id, mrr_date)
ORDER BY
		subscription_id,
		mrr_date ASC