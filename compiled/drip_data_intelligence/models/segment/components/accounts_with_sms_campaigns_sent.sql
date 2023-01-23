WITH sms_campaigns_with_recency AS (
  SELECT
    id,
    account_id,
    sent_at,
    (
      SELECT
        MAX(sent_at)
      FROM
        "current".sms_campaigns
    ) AS max_sent_at,
    (
      sent_at > max_sent_at - 30
    ) AS was_sent_last_30_days,
    (
      sent_at > max_sent_at - 7
    ) AS was_sent_last_7_days
  FROM
    "current".sms_campaigns
  WHERE
    1 = 1
    AND sent_at IS NOT NULL
)
SELECT
  account_id AS drip_account_id,
  COUNT(*) AS drip_sms_campaigns_sent_all_time,
  SUM(
    was_sent_last_30_days :: INTEGER
  ) AS drip_sms_campaigns_sent_last_30_days,
  SUM(
    was_sent_last_7_days :: INTEGER
  ) AS drip_sms_campaigns_sent_last_7_days
FROM
  sms_campaigns_with_recency
GROUP BY
  account_id