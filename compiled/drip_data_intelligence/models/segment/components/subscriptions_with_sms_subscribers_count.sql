SELECT
  subscription_id AS drip_subscription_id,
  COALESCE(SUM(n_sms_people), 0) AS drip_sms_subscriber_count
FROM
  "dumps"."dev"."account_metrics_current" acm
GROUP BY
  subscription_id