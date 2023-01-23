WITH
  subscription_plans AS (
    SELECT
      id AS subscription_id
    , plan_id
    , comped = 't' AS is_comped
    FROM
      "dumps"."dev"."subscriptions"
  )
SELECT
  transaction_id
, r.subscription_id
, 'refunded' AS status
, subscription_plans.plan_id
, NULL AS transaction_status
, DATE(r.created_at) AS date_created_at
, r.created_at :: TIMESTAMP
, NULL :: NUMERIC AS credits_used
, r.amount * 0.01 AS gross_amount_in_USD
, NULL :: NUMERIC AS net_amount_in_USD
, r.amount * 0.01 AS USD_refunded
, NULL :: NUMERIC AS credits_remaining
, r.stripe_charge_id
, t.category
, 'stripe' AS transaction_source
, 'FROM DRIP REFUNDS TABLE' AS memo
, t.amount - t.credit_used AS total_transaction_amount
FROM
  finance.refunds r
  LEFT JOIN subscription_plans USING (subscription_id)
  LEFT JOIN "dumps"."dev"."transactions" t ON t.id = r.transaction_id
WHERE
  category != 'sms_monthly_service'