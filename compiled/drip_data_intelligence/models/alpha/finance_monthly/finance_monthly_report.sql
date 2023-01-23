SELECT
  DISTINCT -- TODO: DISTINCT here is a total kludge. Handles when annual credits are added covering both email and SMS
  transaction_id :: TEXT
, stripe_charge_id
, subscription_id :: TEXT
, status
, plan_id
, CASE
    WHEN category = 'monthly' THEN CASE
      WHEN ABS(gross_amount_in_USD) = 0 THEN 'Starter'
      WHEN (
        ABS(gross_amount_in_USD) > 0
        AND ABS(gross_amount_in_USD) < 40.83
      ) THEN 'Lite'
      WHEN (
        ABS(gross_amount_in_USD) >= 40.83
        AND ABS(gross_amount_in_USD) <= 49.00
      ) THEN 'Basic'
      WHEN (
        ABS(gross_amount_in_USD) > 49.00
        AND ABS(gross_amount_in_USD) <= 122.00
      ) THEN 'Pro'
      WHEN ABS(gross_amount_in_USD) > 122.00 THEN 'High Value'
    END
    WHEN category = 'annual' THEN CASE
      WHEN ABS(gross_amount_in_USD) = 0 THEN 'Starter'
      WHEN (
        ABS(gross_amount_in_USD) > 0
        AND ABS(gross_amount_in_USD) < 408.30
      ) THEN 'Lite'
      WHEN (
        ABS(gross_amount_in_USD) >= 408.30
        AND ABS(gross_amount_in_USD) <= 490.00
      ) THEN 'Basic'
      WHEN (
        ABS(gross_amount_in_USD) > 490.00
        AND ABS(gross_amount_in_USD) <= 1220.00
      ) THEN 'Pro'
      WHEN ABS(gross_amount_in_USD) > 1220.00 THEN 'High Value'
    END
    ELSE 'High Value'
  END AS plan_estimate
, transaction_status
, created_at
, date_created_at
, credits_remaining
, transaction_source
, memo
, category
, gross_amount_in_usd
, credits_used_in_USD AS credits_used
, usd_refunded
, tax_amount_in_usd
, case when status = 'refunded' then 0
       when category = 'annual' then gross_amount_in_usd - tax_amount_in_usd
       else gross_amount_in_usd - credits_used - tax_amount_in_usd
    end as net_usd_after_tax
, case when status = 'refunded' then 0
       when category = 'annual' then gross_amount_in_usd
       else gross_amount_in_usd - credits_used
    end as net_usd_before_tax
FROM
  (
    SELECT "transaction_id",
  "subscription_id",
  "status",
  "plan_id",
  "transaction_status",
  "date_created_at",
  "created_at",
  "credits_used_in_usd",
  "gross_amount_in_usd",
  "net_amount_in_usd",
  "usd_refunded",
  "credits_remaining",
  "stripe_charge_id",
  "category",
  "transaction_source",
  "memo",
  "total_transaction_amount",
  "tax_amount_in_usd"
    FROM
      "dumps"."dev"."finance_monthly_transactions_full"
    UNION
    SELECT "transaction_id",
  "subscription_id",
  "status",
  "plan_id",
  "transaction_status",
  "date_created_at",
  "created_at",
  "credits_used",
  "gross_amount_in_usd",
  "net_amount_in_usd",
  "usd_refunded",
  "credits_remaining",
  "stripe_charge_id",
  "category",
  "transaction_source",
  "memo",
  "total_transaction_amount"
        , 0 as tax
    FROM
      "dumps"."dev"."finance_monthly_refunds_full"
  ) iww
WHERE date_trunc ('mon',created_at) >= '01-jun-2022'
  AND (
    total_transaction_amount != 0
    OR (
      usd_refunded IS NOT NULL
      AND status = 'refunded'
    )
  )
ORDER BY
  created_at