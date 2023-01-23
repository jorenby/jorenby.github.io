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
    , comped = 't' AS is_comped
    FROM
      "dumps"."dev"."subscriptions"
  )
, monthly_service_split AS (
    SELECT
      transaction_id
    , category
    , nvl(amount_cents,0) amount_cents
    , nvl(credits_used,0) credits_used
    , nvl(tax_cents,0) tax_cents
    FROM
		  "dumps"."dev"."monthly_service_transaction_totals_by_category"
    WHERE
      amount_cents > 0
      OR credits_used > 0
  )
, annual_taxes as (
    SELECT iia.transaction_id
         , sum(iia.amount) tax_cents
      FROM "dumps"."dev"."invoice_item_allocations" iia
               LEFT JOIN "dumps"."dev"."invoice_items" ii ON ii.id = iia.invoice_item_id
     WHERE ii.category = 'sales_tax'
  GROUP BY iia.transaction_id
)
SELECT
  t.id AS transaction_id
, t.subscription_id
, new_renewal_status AS status
, transaction_plan_id AS plan_id
, status AS transaction_status
, DATE(t.created_at) AS date_created_at
, t.created_at
, CASE
    WHEN t.category = 'monthly' THEN monthly_service_split.credits_used * 0.01
    ELSE t.credit_used * 0.01
  END AS credits_used_in_USD
, CASE
    WHEN t.category = 'monthly' THEN (
      monthly_service_split.amount_cents + monthly_service_split.credits_used -- Janky. monthly_service_split.amount_cents already has credits deducted, so add them back here
    ) * 0.01
    ELSE t.amount * 0.01
  END AS gross_amount_in_USD
, CASE
    WHEN monthly_service_split.category = 'sms_monthly_service' THEN gross_amount_in_USD
    ELSE gross_amount_in_USD - credits_used_in_USD
  END AS net_amount_in_USD
, NULL :: NUMERIC  AS USD_refunded
, t.credit_remaining * 0.01 AS credits_remaining
, t.stripe_charge_id
, CASE
    WHEN t.category IS NULL THEN 'SMS'
    WHEN t.category = 'monthly' THEN CASE
      WHEN monthly_service_split.category = 'sms_monthly_service' THEN 'sms_monthly'
      ELSE t.category
    END
    ELSE t.category
  END AS category
, CASE
    WHEN t.provider IS NULL and stripe_charge_id is not null THEN 'stripe'
    ELSE provider
  END AS transaction_source
, memo
, t.amount - t.credit_used AS total_transaction_amount
, nvl(case
    when t.category = 'monthly'  then monthly_service_split.tax_cents * 0.01
    else annual_taxes.tax_cents * 0.01
   end, 0) as tax_amount_in_usd
FROM
  "dumps"."dev"."transactions" t
  LEFT JOIN transaction_new_or_renewal_statuses USING (id)
  LEFT JOIN subscription_plans USING (subscription_id)
  LEFT JOIN monthly_service_split ON monthly_service_split.transaction_id = t.id and t.category = 'monthly'
  LEFT JOIN annual_taxes on t.id = annual_taxes.transaction_id
WHERE (stripe_charge_id != 'Drip Test Subscription' or stripe_charge_id is null)