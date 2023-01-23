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
SELECT subscription_id,
       ISNULL(
               ISNULL(
                       NULLIF(oms_indicated, 'Unknown'),
                       oms_detected,
                       'Unknown'
                   )
           ) AS oms_indicated_backfilled
FROM (
      (SELECT subscription_id, oms_indicated FROM oms_indicated_with_rows WHERE row_num = 1) AS oms_indicated
         LEFT JOIN (
    SELECT subscription_id,
           INITCAP(NULLIF(ecomm_platform, 'None')) AS oms_detected
    FROM (
             SELECT subscription_id,
                    ecomm_platform,
                    updated_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY subscription_id
                        ORDER BY
                            updated_at ASC
                        )
             FROM (
                   (
                       SELECT account_public_id AS public_id,
                              ecomm_platform,
                              updated_at
                       FROM current.account_technology_usages
                   ) AS tech_usage
                      JOIN (
                 SELECT subscription_id,
                        public_id
                 FROM "dumps"."dev"."accounts"
             ) AS accounts USING (public_id)
                 )
         ) backfilled_oms_platform
    WHERE ROW_NUMBER = 1
) AS earliest_oms_detected USING (subscription_id))