WITH contracted AS
      (
        SELECT subscription_id
               , date_trunc('d', occurred_at)::date              contracted_date
               , mrr_cents_delta / -100                          contracted_mrr
               , action
               , CASE WHEN action = 'contracted' THEN 1 ELSE -1 END contracted_counter
               , CASE
                     WHEN contracted_mrr <= 250
                         THEN 1
                     WHEN contracted_mrr <= 1200
                         THEN 2
                         ELSE 3 END AS                           category_mrr
               , date_diff('d',
                           date_trunc('d', first_value(occurred_at)
                                           OVER (PARTITION BY subscription_id))::date,
                           contracted_date)
                                                                 active_days
               , CASE
                     WHEN active_days <= 90
                         THEN 'first 90 days'
                     WHEN active_days <= 365
                         THEN '90 to 365 days'
                         ELSE '365+ days' END                    contracted_day_buckets
        FROM "dumps"."dev"."customer_mrr_changed"
        WHERE action = 'contracted'
      )
     , oms_connected AS
      (
          SELECT DISTINCT account_id
                        , TRUE is_oms_connected
            FROM current.integrations
                     JOIN "dumps"."dev"."account_metrics_current" ON account_public_id = account_param
           WHERE deleted_at IS NULL
             AND provider_param = 'bigcommerce'
           UNION
          SELECT DISTINCT account_id
                        , TRUE is_oms_connected
            FROM "dumps"."dev"."account_events"
           WHERE action = 'OMS connected'
      )
     , account_to_subscription AS
      (
          SELECT DISTINCT subscription_id
                        , account_id
                        , is_oms_connected
            FROM "dumps"."dev"."account_metrics_current"
                     LEFT JOIN oms_connected USING (account_id)
      )
     , list_size AS
      (
          SELECT subscription_id
               , occurred_on
               , is_oms_connected
               , sum(billable_subscribers_v2)                                               list_size
               , CASE
                     WHEN list_size < 15000
                         THEN 1
                     WHEN list_size <= 80000
                         THEN 2
                         ELSE 3 END AS                                                      category_list
               , row_number() OVER (PARTITION BY subscription_id ORDER BY occurred_on DESC) n
            FROM current.account_daily_usage_stats
                     JOIN account_to_subscription USING (account_id)
           GROUP BY subscription_id, occurred_on, is_oms_connected
      )
SELECT date_trunc('d', contracted_date)::date                                               contracted_day
     , date_trunc('mon', contracted_date)::date                                             contracted_month
     , CASE
           WHEN category_mrr > nvl(category_list, 0)
               THEN category_mrr
               ELSE category_list END                                                  category
     , CASE
           WHEN category = 1
               THEN 'VSB'
           WHEN category = 2
               THEN 'SMB'
           WHEN category = 3
               THEN 'MM'
    END                                                                                category_name
     , nvl(is_oms_connected, FALSE)                                                    is_ecommerce
     , contracted_day_buckets
     , drip_success_manager AS                                                         is_success_managed
     , CASE
           WHEN category = 1 AND is_ecommerce
               THEN 'VSB (ecom)'
           WHEN category = 2 AND is_ecommerce
               THEN 'SMB (ecom)'
           WHEN category = 3 AND is_ecommerce
               THEN 'MM (ecom)'
           WHEN category = 1 AND NOT is_ecommerce
               THEN 'VSB (non ecom)'
           WHEN category = 2 AND NOT is_ecommerce
               THEN 'SMB (non ecom)'
           WHEN category = 3 AND NOT is_ecommerce
               THEN 'MM (non ecom)' END                                                category_and_ecommerce
     , sum(contracted_mrr)                                                             contracted_amount
     , sum(contracted_counter)                                                         contracted_count
     , sum(contracted_amount)
       OVER (PARTITION BY contracted_month, category_and_ecommerce)                    month_category_and_segment_contracted_amount
     , sum(contracted_count)
       OVER (PARTITION BY contracted_month, category_and_ecommerce)                    month_category_and_segment_contracted_count
     , sum(contracted_amount) OVER (PARTITION BY contracted_month, is_ecommerce)                 month_segment_contracted_amount
     , sum(contracted_count) OVER (PARTITION BY contracted_month, is_ecommerce)                  month_segment_contracted_count
     , sum(contracted_amount) OVER (PARTITION BY contracted_month)                               month_contracted_amount
     , sum(contracted_count) OVER (PARTITION BY contracted_month)                                month_contracted_count
     , month_category_and_segment_contracted_amount / nullif(month_segment_contracted_amount, 0) contracted_amount_pct_of_segment
     , contracted_amount / nullif(month_segment_contracted_amount, 0)                            contracted_amount_category_pct_of_segment
     , contracted_amount / nullif(month_contracted_amount, 0)                                    contracted_amount_pct
     , month_segment_contracted_amount / nullif(month_contracted_amount, 0)                      contracted_amount_segment_pct
  FROM contracted
           LEFT JOIN list_size USING (subscription_id)
           LEFT JOIN "dumps"."dev"."subscriptions_with_success_managers" ON subscription_id = drip_subscription_id
 WHERE action = 'contracted'
   AND (n = 1 OR n IS NULL)
 GROUP BY contracted_day, contracted_month, is_success_managed, category, is_oms_connected, contracted_day_buckets