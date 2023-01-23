SELECT *
  FROM "dumps"."dev"."segment_filtered_users"
 WHERE TRUE
   AND (
         segment_filtered_users.subscription_status <> 'churned'

         -- Include churned users who have had a status change in the last 31 days
         -- All subscriptions eventually get set to `cancelled` in the Monolith and
         -- we want to make sure that gets overwritten with `churned`
         OR segment_filtered_users.status_last_changed_at >= 
    

    
        CURRENT_DATE
    
 - INTERVAL '31 DAYS'
     )
   AND segment_filtered_users.created_by_drip_employee = FALSE