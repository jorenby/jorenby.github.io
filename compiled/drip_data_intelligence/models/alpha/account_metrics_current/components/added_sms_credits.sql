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