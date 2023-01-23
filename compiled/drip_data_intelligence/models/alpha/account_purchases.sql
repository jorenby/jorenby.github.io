WITH account AS
      (
          SELECT DISTINCT id account_id, public_id account_public_id
            FROM "dumps"."dev"."accounts"
      )
  SELECT account_id
       , account_public_id
       , sum(amount_usd)                                                   gross_revenue_amount_usd
       , sum(revenue_amount_usd)                                           net_revenue_amount_usd
       , sum(discount_usd)                                                 discount_amount_total_usd
       , sum(shipping_usd)                                                 shipping_amount_total_usd
       , sum(tax_usd)                                                      tax_amount_total_usd
       , min(occurred_at)                                                          first_purchase_date
       , max(occurred_at)                                                          most_recent_purchase_date
       , date_diff('d', min(occurred_at), current_date)::real / 30                 months_since_first_order
       , date_diff('d', max(occurred_at), current_date)::real / 30                 months_since_most_recent_order
       , avg(nvl(revenue_amount_usd, 0))                                           aov
       , (discount_amount_total_usd / nullif(net_revenue_amount_usd + discount_amount_total_usd, 0)) * 100 discount_pct
       , nvl(count(amount_usd), 0)                                                 count_of_orders
       , count(*)                                                                  purchase_count
       , sum(case when date_diff('d', occurred_at, current_date) <= 365
                  then nvl(revenue_amount_usd,0) else 0 end)                       revenue_rolling_year
    FROM "dumps"."dev"."purchases_corrected"
              JOIN account USING (account_id)
   WHERE amount_usd > 0
     AND canceled_at IS NULL
   GROUP BY account_id, account_public_id