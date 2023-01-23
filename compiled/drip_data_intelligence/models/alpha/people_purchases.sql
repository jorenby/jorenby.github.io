WITH active_accounts AS
      (
          SELECT DISTINCT id account_id, public_id account_public_id
            FROM "dumps"."dev"."accounts"
           WHERE status = 'active'
      )
  SELECT account_id
       , account_public_id
       , subscriber_id
       , sum(nvl(amount_usd, 0))                                                                  gross_revenue_amount
       , sum(nvl(revenue_amount_usd, 0))                                                          net_revenue_amount
       , sum(nvl(discount_usd, 0))                                                                discount_amount
       , sum(nvl(shipping_usd, 0))                                                                shipping_amount
       , sum(nvl(tax_usd, 0))                                                                     tax_amount
       , min(occurred_at)                                                                         first_purchase_date
       , max(occurred_at)                                                                         most_recent_purchase_date
       , date_diff('d', min(occurred_at), current_date)::real / 30                                months_since_first_order
       , date_diff('d', max(occurred_at), current_date)::real / 30                                months_since_most_recent_order
       , avg(nvl(revenue_amount_usd, 0))                                                          aov
       , (discount_amount / nullif(net_revenue_amount + discount_amount, 0)) * 100                discount_pct
       , CASE
             WHEN min(nvl(discount_usd,0)) > 0
                 THEN TRUE
                 ELSE FALSE END                                                                   is_discount_exclusive
       , nvl(count(amount_usd), 0)                                                                count_of_orders
       , sum(net_revenue_amount)
         OVER (PARTITION BY account_id ORDER BY net_revenue_amount DESC ROWS UNBOUNDED PRECEDING) descending_ltv_running_total_by_account
       , sum(net_revenue_amount) OVER (PARTITION BY account_id)                                   total_account_revenue
       , descending_ltv_running_total_by_account / nullif(total_account_revenue, 0)               ranked_rev_pct
    FROM active_accounts
    JOIN "dumps"."dev"."purchases_corrected" USING (account_id)
   WHERE amount_usd > 0
     AND canceled_at IS NULL
   GROUP BY account_id, account_public_id, subscriber_id