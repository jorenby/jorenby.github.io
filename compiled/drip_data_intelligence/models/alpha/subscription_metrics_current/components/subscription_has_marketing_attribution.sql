SELECT CAST(drip_subscription_id__c AS INT)                     AS subscription_id,
       TRUE AS has_marketing_attribution
FROM sales_operations.subscription_attribution
WHERE campaign_category__c = 'TACTIC' AND subscription_tactic_u_shape_attribution IS NOT NULL
AND tactic__c IS NOT NULL
GROUP BY drip_subscription_id__c