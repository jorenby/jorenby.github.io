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