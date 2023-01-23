SELECT id                                              AS subscription_id,
       primary_email LIKE '%@drip.com' OR comped = 't' AS created_by_drip_employee
FROM "dumps"."dev"."subscriptions"
         INNER JOIN (SELECT id AS user_id, email AS primary_email FROM "dumps"."dev"."users") USING (user_id)