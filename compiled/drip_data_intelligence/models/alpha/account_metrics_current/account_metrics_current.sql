WITH
     __dbt__cte__300th_sms_person as (
SELECT account_id,
       TRUE AS has_at_least_300_sms_people,
       sms_person_added_at AS recieved_300_sms_people_at
       FROM
(SELECT account_id,
       ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY created_at) AS row_num,
       created_at AS sms_person_added_at
FROM "dumps"."current"."sms_people")
WHERE row_num = 300
),  __dbt__cte__added_sms_credits as (
SELECT
  account_id,
  MIN(created_at) AS first_added_sms_credits_at,
  TRUE AS has_added_sms_credits
FROM
  "dumps"."current"."sms_credit_rollups"
WHERE
  1 = 1
  AND credits_purchased > 0
GROUP BY
  account_id
),  __dbt__cte__first_attributed_revenue as (
SELECT account_id, MIN(occurred_at) AS first_attributed_purchase_at
FROM "dumps"."dev"."purchases"
WHERE attributed_sms_delivery_id IS NOT NULL
   OR attributed_delivery_id IS NOT NULL
GROUP BY account_id
),  __dbt__cte__first_sent_sms as (
SELECT
  account_id,
  MIN(usage_date) AS first_sent_sms_at,
  TRUE AS has_sent_sms
FROM
  "dumps"."current"."sms_credit_rollups"
WHERE
  1 = 1
  AND sms_messages > 0
GROUP BY
  account_id
),  __dbt__cte__first_sms_people as (
SELECT account_id,
       MIN(created_at) AS first_sms_person_at
FROM "dumps"."current"."sms_people"
GROUP BY account_id
),  __dbt__cte__has_purchases as (
SELECT
  a.account_id,
  p.account_id IS NOT NULL as has_purchases
FROM
  (
    SELECT
      id AS account_id
    FROM
      "dumps"."dev"."accounts"
  ) a
  LEFT JOIN (
    SELECT
      DISTINCT account_id
    FROM
      "dumps"."dev"."purchases"
  ) p USING (account_id)
),  __dbt__cte__has_reserved_long_code as (
SELECT
	ts.account_id AS account_id,
	MIN(tfc.created_at) AS reserved_long_code_at,
	(reserved_long_code_at IS NOT NULL) AS has_reserved_long_code
FROM
	"dumps"."current"."twilio_subaccounts" ts
	LEFT JOIN "dumps"."current"."twilio_from_codes" tfc ON twilio_subaccount_id = ts.id
GROUP BY
	ts.account_id
),  __dbt__cte__mrr as (
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
),  __dbt__cte__oms_indicated as (
WITH most_recent_oms_indicated AS (
      SELECT account_id
           , oms_indicated
           , row_number() OVER (PARTITION BY account_id ORDER BY occurred_at DESC) n
        FROM "dumps"."data_intelligence"."oms_indicated_account_events"
  )
SELECT account_id
     , oms_indicated
  FROM most_recent_oms_indicated
 WHERE n = 1
),  __dbt__cte__sms_forms_fields as (
SELECT
f.account_id AS account_id
, TRUE AS has_sms_optin_form
, MIN(cf.created_at) AS created_sms_option_form_at
FROM "dumps"."dev"."forms" AS f
	JOIN "dumps"."current"."custom_fields" cf
	ON f.id = cf.form_id
WHERE 1 = 1
AND cf.data_type = 'phone'
AND cf.is_consent_asked = TRUE
GROUP BY 1, 2
),  __dbt__cte__sms_people as (
SELECT account_id,
       TRUE as has_sms_people,
       COUNT(*) AS n_sms_people
FROM "dumps"."current"."sms_people"
WHERE status = 'opted_in'
GROUP BY account_id
),  __dbt__cte__total_billable_subscribers as (
SELECT
  account_id,
  billable_subscribers_v2 AS total_billable_subscribers
FROM
  (
    SELECT
      account_id,
      billable_subscribers_v2,
      ROW_NUMBER() OVER (
        PARTITION BY account_id
        ORDER BY
          occurred_on DESC
      ) AS row_num
    FROM
      "dumps"."current"."account_daily_usage_stats"
  )
WHERE
  row_num = 1
),base AS (
        SELECT
            id        account_id
          , public_id account_public_id
          , subscription_id
          , name
          , url
          , created_at
        FROM "dumps"."dev"."accounts"
    )
SELECT account_id
     , account_public_id
     , created_at
     , created_sms_option_form_at
     , estimated_mrr
     , first_added_sms_credits_at
     , first_attributed_purchase_at
     , first_sent_sms_at
     , first_sms_person_at
     , has_added_sms_credits
     , has_at_least_300_sms_people
     , has_purchases
     , has_reserved_long_code
     , has_sent_sms
     , has_sms_optin_form
     , has_sms_people
     , n_sms_people
     , name
     , oms_indicated
     , recieved_300_sms_people_at
     , reserved_long_code_at
     , subscription_id
     , total_billable_subscribers
     , url
FROM base
     LEFT JOIN __dbt__cte__300th_sms_person USING (account_id)
     LEFT JOIN __dbt__cte__added_sms_credits USING (account_id)
     LEFT JOIN __dbt__cte__first_attributed_revenue USING (account_id)
     LEFT JOIN __dbt__cte__first_sent_sms USING (account_id)
     LEFT JOIN __dbt__cte__first_sms_people USING (account_id)
     LEFT JOIN __dbt__cte__has_purchases USING (account_id)
     LEFT JOIN __dbt__cte__has_reserved_long_code USING (account_id)
     LEFT JOIN __dbt__cte__mrr USING (account_id)
     LEFT JOIN __dbt__cte__oms_indicated USING (account_id)
     LEFT JOIN __dbt__cte__sms_forms_fields USING (account_id)
     LEFT JOIN __dbt__cte__sms_people USING (account_id)
     LEFT JOIN __dbt__cte__total_billable_subscribers USING (account_id)