-- This is a refactor of https://metabase.insights.drip.sh/question/49-finance-transaction-record
WITH
  transaction_new_or_renewal_statuses AS (
    SELECT
      id
    , ROW_NUMBER() OVER (
        PARTITION BY subscription_id
        ORDER BY
          created_at
      ) AS row_num
    , CASE
        WHEN row_num = 1 THEN 'New'
        ELSE 'Renewal'
      END AS new_renewal_status
    FROM
      "dumps"."dev"."transactions"
  )
, subscription_plans AS (
    -- Janky. For billing version 6, new/renewal transactions get a different plan id than refunds do
    SELECT
      id AS subscription_id
    , CASE
        WHEN billing_version = 6 THEN CONCAT('Plan ', plan_id)
        ELSE plan_id
      END AS transaction_plan_id
    , plan_id AS refund_plan_id
    FROM
      "dumps"."dev"."subscriptions"
  )
, transactions AS (
    SELECT
      t.id AS transaction_id
    , t.subscription_id
    , new_renewal_status AS status
    , transaction_plan_id AS plan_id
    , status AS transaction_status
    , DATE(t.created_at) AS date_created_at
    , t.created_at
    , t.amount * 0.01 AS gross_amount_in_USD
    , credit_used * 0.01 AS credits_used
    , (t.amount - credit_used) * 0.01 AS net_amount_in_USD
    , NULL AS USD_refunded
    , credit_remaining * 0.01 AS credits_remaining
    , t.stripe_charge_id
    , CASE
        WHEN category IS NULL THEN 'SMS'
        ELSE category
      END AS category
    , CASE
        WHEN t.provider IS NULL THEN 'stripe'
        ELSE provider
      END AS transaction_source
    , memo
    FROM
      "dumps"."dev"."transactions" t
      LEFT JOIN transaction_new_or_renewal_statuses USING (id)
      LEFT JOIN subscription_plans USING (subscription_id)
  )
, refunds AS (
    SELECT
      transaction_id
    , r.subscription_id
    , 'refunded' AS status
    , refund_plan_id AS plan_id
    , NULL AS transaction_status
    , DATE(r.created_at) AS date_created_at
    , r.created_at :: TIMESTAMPTZ
    , r.amount * 0.01 AS gross_amount_in_USD
    , NULL AS credits_used
    , NULL AS net_amount_in_USD
    , r.amount * 0.01 AS USD_refunded
    , NULL AS credits_remaining
    , r.stripe_charge_id
    , transactions.category
    , 'stripe' AS transaction_source
    , 'FROM DRIP REFUNDS TABLE' AS memo
    FROM
      finance.refunds r
      LEFT JOIN subscription_plans USING (subscription_id)
      LEFT JOIN transactions USING (transaction_id)
  )
SELECT
  transaction_id :: TEXT
, stripe_charge_id
, subscription_id :: TEXT
, status
, plan_id
,
--  CASE
--     WHEN category = 'monthly' THEN CASE
--       WHEN ABS(gross_amount_in_USD) = 0 THEN 'Starter'
--       WHEN (
--         ABS(gross_amount_in_USD) > 0
--         AND ABS(gross_amount_in_USD) < 40.83
--       ) THEN 'Lite'
--       WHEN (
--         ABS(gross_amount_in_USD) >= 40.83
--         AND ABS(gross_amount_in_USD) <= 49.00
--       ) THEN 'Basic'
--       WHEN (
--         ABS(gross_amount_in_USD) > 49.00
--         AND ABS(gross_amount_in_USD) <= 122.00
--       ) THEN 'Pro'
--       WHEN ABS(gross_amount_in_USD) > 122.00 THEN 'High Value'
--     END
--     WHEN category = 'annual' THEN CASE
--       WHEN ABS(gross_amount_in_USD) = 0 THEN 'Starter'
--       WHEN (
--         ABS(gross_amount_in_USD) > 0
--         AND ABS(gross_amount_in_USD) < 408.30
--       ) THEN 'Lite'
--       WHEN (
--         ABS(gross_amount_in_USD) >= 408.30
--         AND ABS(gross_amount_in_USD) <= 490.00
--       ) THEN 'Basic'
--       WHEN (
--         ABS(gross_amount_in_USD) > 490.00
--         AND ABS(gross_amount_in_USD) <= 1220.00
--       ) THEN 'Pro'
--       WHEN ABS(gross_amount_in_USD) > 1220.00 THEN 'High Value'
--     END
--     ELSE 'High Value'
--   END
  NULL AS plan_estimate -- NOTE: this is to be able to compare against the new report, rolled up
, transaction_status
, created_at
, date_created_at
, credits_remaining
, transaction_source
, memo
, category
, gross_amount_in_usd
, credits_used
, usd_refunded
FROM
  (
    SELECT
      *
    FROM
      transactions
    UNION
    SELECT
      *
    FROM
      refunds
  )
WHERE
  EXTRACT(
    YEAR
    FROM
      created_at
  ) = 2022
  -- 	AND EXTRACT(MONTH
  -- FROM
  -- 	created_at) = 07
  AND (
    net_amount_in_usd != 0
    OR usd_refunded IS NOT NULL
  )
ORDER BY
  created_at