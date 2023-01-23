WITH daily_kpi_prep AS
      (
          SELECT *
               , trunc(
                          last_value(active_mrr_cents :: decimal / 100) OVER (
                      PARTITION BY date_trunc('mon', mrr_date), category
                      ORDER BY
                          mrr_date ROWS BETWEEN UNBOUNDED PRECEDING
                          AND UNBOUNDED FOLLOWING
                      ),
                          2
              )        AS ending_mrr_usd
               , last_value(active_count)
                 OVER (PARTITION BY date_trunc('mon', mrr_date), category ORDER BY mrr_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                     ) AS ending_number_of_customers
            FROM "dumps"."dev"."daily_kpi_by_category"
           ORDER BY mrr_date DESC
      )
     , top_of_funnel AS
      (
          SELECT date_trunc('mon', ssp.drip_registration_date)::date                    AS as_of_month
               , sum(CASE WHEN drip_trial_start_date IS NOT NULL THEN 1 ELSE 0 END)     AS trial_count
               , sum(CASE WHEN subscription_status IN ('setting_up') THEN 1 ELSE 0 END) AS trial_abandoned
               , sum(CASE
                         WHEN subscription_status NOT IN ('setting_up', 'banned')
                             THEN 1
                             ELSE 0 END)                                                AS trial_onboarded
               , sum(CASE WHEN drip_is_oms_indicated THEN 1 ELSE 0 END)                 AS trial_with_oms
            FROM "dumps"."dev"."segment_subscription_properties" AS ssp
           WHERE as_of_month IS NOT NULL
           GROUP BY as_of_month
      )
     , monthly_mrr_cte AS
      (
          SELECT date_trunc('mon', mrr_date) :: date                                        AS as_of_month
               , category
               , trunc(sum(activation_mrr_cents :: decimal) / 100, 2)                       AS gross_new_mrr_usd
               , trunc(sum(expansion_mrr_cents :: decimal) / 100, 2)                        AS expansion_mrr_usd
               , trunc(sum(contraction_mrr_cents :: decimal) / 100, 2)                      AS contraction_mrr_usd
               , trunc(expansion_mrr_usd + contraction_mrr_usd, 2)                          AS net_expansion_mrr_usd
               , trunc((sum(churn_mrr_cents :: decimal) + sum(reactivation_mrr_cents :: decimal)) / 100,
                       2)                                                                   AS churned_mrr_usd
               , gross_new_mrr_usd + expansion_mrr_usd + contraction_mrr_usd +
                 churned_mrr_usd                                                            AS net_new_mrr_usd
               , ending_mrr_usd
               , lag(ending_mrr_usd) OVER ( PARTITION BY category ORDER BY as_of_month)     AS starting_mrr_usd
               , (net_new_mrr_usd / nullif(starting_mrr_usd, 0)) * 100                      AS monthly_mrr_growth_pct
               , ending_mrr_usd * 12                                                        AS ending_arr_usd
               , (abs(churned_mrr_usd) / nullif(starting_mrr_usd, 0)) * 100                 AS mrr_gross_churn_pct
               , (net_expansion_mrr_usd / nullif(starting_mrr_usd, 0)) * 100                AS mrr_net_expansion_pct
               , ((net_expansion_mrr_usd + churned_mrr_usd) / nullif(starting_mrr_usd, 0)) *
                 -100                                                                       AS net_mrr_churn_pct
               , 100 - net_mrr_churn_pct                                                    AS net_revenue_retention_pct
               , lag(ending_arr_usd, 12) OVER (PARTITION BY category ORDER BY as_of_month ) AS ending_arr_usd_prior_year
               , (ending_arr_usd - ending_arr_usd_prior_year) /
                 nullif(ending_arr_usd_prior_year, 0)                                       AS arr_yoy_growth
               , (gross_new_mrr_usd + expansion_mrr_usd) /
                 nullif(abs(contraction_mrr_usd + churned_mrr_usd), 0)                      AS quick_ratio
            FROM daily_kpi_prep
           GROUP BY as_of_month, ending_mrr_usd, category
      )
     , monthly_customer AS
      (
          SELECT date_trunc('mon', mrr_date) :: date AS                as_of_month
               , category
               , sum(activation_count) AS                              gross_new_customers
               , sum(expansion_count) AS                               expanded_customers
               , sum(abs(contraction_count)) AS                        contracted_customers
               , sum(reactivation_count) - sum(churn_count) AS         churned_customers
               , ending_number_of_customers
               , lag(ending_number_of_customers)
                 OVER (PARTITION BY category ORDER BY as_of_month ) AS starting_number_of_customers
               , gross_new_customers + churned_customers AS            net_new_customers
               , (net_new_customers :: decimal / nullif(starting_number_of_customers, 0) :: decimal) *
                 100 AS                                                monthly_customer_growth_pct
               , (abs(churned_customers :: decimal) / nullif(starting_number_of_customers, 0) :: decimal) *
                 100 AS                                                customer_churn_pct
               , lag(ending_number_of_customers, 12)
                 OVER (PARTITION BY category ORDER BY as_of_month ) AS ending_number_of_customers_prior_year
               , (ending_number_of_customers - ending_number_of_customers_prior_year) /
                 nullif(ending_number_of_customers_prior_year, 0) AS   customer_yoy_growth
            FROM daily_kpi_prep
           GROUP BY as_of_month, ending_number_of_customers, category
      )
SELECT as_of_month
     , category
     , gross_new_mrr_usd
     , expansion_mrr_usd
     , contraction_mrr_usd
     , churned_mrr_usd
     , net_new_mrr_usd
     , monthly_mrr_growth_pct
     , starting_mrr_usd
     , ending_mrr_usd
     , ending_arr_usd
     , gross_new_mrr_usd :: decimal / nullif(gross_new_customers, 0) :: decimal     AS avg_mrr_per_new_customer_usd
     , ending_mrr_usd :: decimal / nullif(ending_number_of_customers, 0) :: decimal AS avg_mrr_installed_base_usd
     , avg_mrr_installed_base_usd * 12                                              AS avg_arr_installed_base_usd
     , gross_new_customers
     , expanded_customers
     , contracted_customers
     , churned_customers
     , net_new_customers
     , monthly_customer_growth_pct
     , starting_number_of_customers
     , ending_number_of_customers
     , customer_churn_pct
     , mrr_gross_churn_pct
     , mrr_net_expansion_pct
     , net_mrr_churn_pct
     , net_revenue_retention_pct
     , arr_yoy_growth
     , customer_yoy_growth
     , quick_ratio
     , trial_count
     , trial_abandoned
     , trial_onboarded
     , trial_with_oms
  FROM monthly_mrr_cte
           JOIN monthly_customer USING (as_of_month, category)
           LEFT JOIN top_of_funnel USING (as_of_month)
 ORDER BY as_of_month DESC