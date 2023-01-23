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