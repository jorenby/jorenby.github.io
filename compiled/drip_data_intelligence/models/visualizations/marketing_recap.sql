WITH clicks AS
      (
          SELECT email_id
               , sum(CASE WHEN action = 'click' THEN 1 ELSE 0 END) click_count
            FROM "dumps"."current"."delivery_events"
           WHERE action = 'click'
           GROUP BY account_id, email_id, emailable_type
      )
     , email AS
      (
          SELECT account_id
               , email_id
               , emailable_type --'AutomationEmail' for automations 'Broadcast' for broadcasts
               , sum(CASE WHEN action = 'deliver' THEN 1 ELSE 0 END)   delivery_count
               , click_count
               , click_count::float / nullif(delivery_count, 0)::float click_through_rate
            FROM "dumps"."current"."delivery_events"
                     LEFT JOIN clicks USING (email_id)
           WHERE action = 'deliver'
           GROUP BY account_id, email_id, emailable_type, click_count
      )
     , purchases AS
      (
          SELECT attributed_email_id     email_id
               , sum(revenue_amount_usd) revenue_total_by_email
               , count(*)                order_count_by_email
               , sum(CASE
                         WHEN attributed_emailable_id IS NOT NULL OR attributed_automation_id IS NOT NULL OR
                              attributed_sms_delivery_id IS NOT NULL OR attributed_delivery_id IS NOT NULL
                             THEN 1
                             ELSE 0 END) attributed_count_by_email
            FROM "dumps"."dev"."purchases_corrected"
           GROUP BY email_id
      )
     , email_name AS
      (
          SELECT email_id, id, name, sent_at
            FROM "dumps"."dev"."broadcasts"
           UNION ALL
          SELECT email_id, workflows.public_id, workflows.name, NULL
            FROM "dumps"."dev"."automation_emails"
                     JOIN "dumps"."dev"."workflows" ON automation_id = workflows.id
      )
     , verticals AS
      (SELECT account_id
            , display_name industry_category_name
         FROM current.account_industry_categories
                  JOIN current.industry_categories ON industry_categories.id = industry_category_id)
SELECT extract(YEAR FROM sent_at)     as year
     , left(industry_category_name, 100) industry_category_name
     , account_id
     , email_id
     , id                                broadcast_or_automation_id
    --, left(name, 100)                   name
     , name
    --, left(emailable_type, 100)         emailable_type
     , emailable_type
     , delivery_count
     , click_count
     , click_through_rate
     , revenue_total_by_email
     , attributed_count_by_email
     , order_count_by_email
  FROM email
           LEFT JOIN purchases USING (email_id)
           LEFT JOIN email_name USING (email_id)
           LEFT JOIN verticals USING (account_id)