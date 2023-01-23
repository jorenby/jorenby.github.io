SELECT CAST(drip_subscription_id__c AS INT)                     AS subscription_id,
       SUM(subscription_tactic_u_shape_attribution) AS pct_trial_lead_attributed_to_paid_marketing
FROM sales_operations.subscription_attribution
WHERE campaign_category__c = 'TACTIC' AND subscription_tactic_u_shape_attribution IS NOT NULL
AND tactic__c IN ('Paid Search', 'Paid Social', 'Display')
GROUP BY drip_subscription_id__c