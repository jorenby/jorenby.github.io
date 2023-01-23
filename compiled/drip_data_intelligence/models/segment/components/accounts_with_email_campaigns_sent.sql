SELECT
  account_id AS drip_account_id,
  campaigns_sent_total AS drip_email_campaigns_sent_all_time,
  campaigns_sent_7_days AS drip_email_campaigns_sent_last_7_days,
  campaigns_sent_30_days AS drip_email_campaigns_sent_last_30_days
FROM
  data_intelligence.product_usage_snapshot_most_recent_day