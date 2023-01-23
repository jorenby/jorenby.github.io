WITH subscription_metrics_current_without_oms AS (
    SELECT *
    FROM (
             SELECT *
             FROM current.subscription_dimensions) sd
             INNER JOIN (
        SELECT *
        FROM "dumps"."current"."subscription_details") s_det USING (subscription_id)
),
     oms_indicated_with_rows AS (
         SELECT smc.*
              , ois.oms_indicated
              , ROW_NUMBER() OVER (PARTITION BY smc.subscription_id ORDER BY occurred_at DESC) AS row_num
         FROM subscription_metrics_current_without_oms smc
                  LEFT JOIN data_intelligence.oms_indicated_subscription_events ois
                            ON ois.subscription_id = smc.subscription_id AND ois.occurred_at <= smc.trial_end_at
     )
SELECT subscription_id, oms_indicated FROM oms_indicated_with_rows WHERE row_num = 1