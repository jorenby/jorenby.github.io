SELECT s.subscription_id
FROM "dumps"."dev"."subscription_id" s
         LEFT JOIN "dumps"."dev"."subscription_metrics_current" smc USING (subscription_id)
WHERE smc.subscription_id IS NULL