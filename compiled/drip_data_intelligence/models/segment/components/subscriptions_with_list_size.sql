WITH todays_stats AS (
  SELECT
    account_id AS id,
    billable_subscribers_v2
  FROM
    current.account_daily_usage_stats
  WHERE
    TRUE
    AND created_at :: DATE = (
    

    
        CURRENT_DATE
    
 - INTERVAL '1 DAY') :: DATE
)
SELECT
  a.subscription_id AS drip_subscription_id,
  SUM(billable_subscribers_v2) AS drip_email_list_size
FROM
  "dumps"."dev"."accounts" a
  JOIN todays_stats USING (id)
GROUP BY
  1