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