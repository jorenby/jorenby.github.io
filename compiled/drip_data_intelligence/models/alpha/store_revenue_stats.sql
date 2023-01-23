WITH account_daily_revenue AS
           (
               SELECT account_id
                    , occurred_at
                    , first_date_of_month
                    , calendar_date = last_date_of_month                                                     is_last_date_of_month
                    , dense_rank()
                      OVER (PARTITION BY account_id, is_last_date_of_month ORDER BY last_date_of_month DESC) month_count_desc
                    , first_date_of_year
                    , last_date_of_year
                    , coalesce(total_revenue_USD, 0)                                                         revenue_account_daily_usd
                    , sum(revenue_account_daily_usd)
                      OVER (PARTITION BY account_id, first_date_of_month)                                    revenue_account_monthly_usd
                    , sum(revenue_account_daily_usd)
                      OVER (PARTITION BY account_id, first_date_of_year)                                     revenue_account_annual_usd
                 FROM "dumps"."alpha"."dim_date" d
                          LEFT JOIN "dumps"."dev"."account_attributed_revenue" aar ON calendar_date = occurred_at
           )
SELECT subscription_id
     , account_id
     , avg(revenue_account_daily_usd)                    revenue_account_daily_average_usd
     , avg(CASE
               WHEN is_last_date_of_month
                   THEN revenue_account_monthly_usd END) revenue_account_monthly_average_usd
     , avg(CASE
               WHEN occurred_at = last_date_of_year
                   THEN revenue_account_annual_usd END)  revenue_account_annual_average_usd
     , sum(CASE
               WHEN date_diff('d', occurred_at, current_date) <= 365
                   THEN revenue_account_daily_usd END)   revenue_account_rolling_year_usd
     , sum(CASE
               WHEN is_last_date_of_month
                   AND month_count_desc <= 12
                   THEN revenue_account_monthly_usd END) revenue_account_last_12_month_avgerage_usd
     , sum(CASE
               WHEN first_date_of_year = date_trunc('y', current_date)
                   THEN revenue_account_daily_usd END)   revenue_account_ytd_usd
     , sum(revenue_account_daily_average_usd)
       OVER (PARTITION BY a.subscription_id)             revenue_subscription_daily_average_usd
     , sum(revenue_account_monthly_average_usd)
       OVER (PARTITION BY a.subscription_id)             revenue_subscription_monthly_average_usd
     , sum(revenue_account_annual_average_usd)
       OVER (PARTITION BY a.subscription_id)             revenue_subscription_annual_average_usd
     , sum(revenue_account_rolling_year_usd)
       OVER (PARTITION BY a.subscription_id)             revenue_subscription_rolling_year_usd
     , sum(revenue_account_last_12_month_avgerage_usd)
       over(PARTITION BY  a.subscription_id)             revenue_subscription_last_12_month_average_usd
     , sum(revenue_account_ytd_usd)
       OVER (PARTITION BY a.subscription_id)             revenue_subscription_ytd_usd
  FROM account_daily_revenue adr
           LEFT JOIN "dumps"."dev"."accounts" a ON adr.account_id = a.id
 GROUP BY account_id, subscription_id