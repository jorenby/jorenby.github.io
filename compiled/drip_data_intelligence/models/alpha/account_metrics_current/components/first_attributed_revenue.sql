SELECT account_id, MIN(occurred_at) AS first_attributed_purchase_at
FROM "dumps"."dev"."purchases"
WHERE attributed_sms_delivery_id IS NOT NULL
   OR attributed_delivery_id IS NOT NULL
GROUP BY account_id