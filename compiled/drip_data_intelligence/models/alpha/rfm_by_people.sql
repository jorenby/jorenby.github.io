WITH subscriber_agg AS
        (
            SELECT account_id
                 , subscriber_id
                 , gross_revenue_amount
                 , net_revenue_amount
                 , discount_amount
                 , shipping_amount
                 , tax_amount
                 , first_purchase_date
                 , most_recent_purchase_date
                 , months_since_first_order
                 , months_since_most_recent_order
                 , aov
                 , net_revenue_amount ltv
                 , discount_amount    lifetime_discounts
                 , is_discount_exclusive
                 , count_of_orders
              FROM "dumps"."dev"."people_purchases" --alpha.people_purchases using the ref gets me a runtime error on dbt power user
        )
       , rfm AS
        (
            SELECT account_id
                 , subscriber_id
                 , CASE
                       WHEN ltv != 0
                           THEN count(subscriber_id)
                                OVER (PARTITION BY account_id) END                             total_purchasers
                --r
                 , months_since_most_recent_order
                 , rank()
                   OVER (PARTITION BY account_id ORDER BY months_since_most_recent_order DESC) subscriber_recency_rank
                 , subscriber_recency_rank::real / nullif(total_purchasers::real, 0)           recency_rank_pct
                 , CASE
                       WHEN recency_rank_pct < .2
                           THEN 1
                       WHEN recency_rank_pct < .4
                           THEN 2
                       WHEN recency_rank_pct < .6
                           THEN 3
                       WHEN recency_rank_pct < .8
                           THEN 4
                           ELSE 5 END                                                          recency
                --f
                 , count_of_orders
                 , rank() OVER (PARTITION BY account_id ORDER BY count_of_orders)::real
                / nullif(total_purchasers, 0)                                                  repeat_buyer_percentile
                 , CASE
                       WHEN repeat_buyer_percentile IS NULL
                           THEN 0
                       WHEN repeat_buyer_percentile < .2
                           THEN 1
                       WHEN repeat_buyer_percentile < .4
                           THEN 2
                       WHEN repeat_buyer_percentile < .6
                           THEN 3
                       WHEN repeat_buyer_percentile < .8
                           THEN 4
                           ELSE 5 END                                                          frequency
                --m
                 , ltv
                 , rank() OVER (PARTITION BY account_id ORDER BY ltv)                          subscriber_ltv_rank
                 , subscriber_ltv_rank::real / nullif(total_purchasers::real, 0)               ltv_rank_percent
                 , CASE
                       WHEN ltv = 0
                           THEN 0
                       WHEN ltv_rank_percent < .2
                           THEN 1
                       WHEN ltv_rank_percent < .4
                           THEN 2
                       WHEN ltv_rank_percent < .6
                           THEN 3
                       WHEN ltv_rank_percent < .8
                           THEN 4
                           ELSE 5 END                                                          monetary
                --rfm calcs
                 , recency || frequency || monetary                                            rfm
                --other measures
                 , months_since_first_order
                 , aov
                 , lifetime_discounts
              FROM subscriber_agg
             ORDER BY account_id, subscriber_id)
  SELECT account_id
       , subscriber_id
       , CASE
             WHEN rfm = 555 OR rfm = 554 OR rfm = 544 OR rfm = 545 OR rfm = 454 OR rfm = 455 OR rfm = 445
                 THEN 1
             WHEN rfm = 543 OR rfm = 444 OR rfm = 435 OR rfm = 355 OR rfm = 354 OR rfm = 345 OR rfm = 344 OR rfm = 335
                 THEN 2
             WHEN rfm = 553 OR rfm = 551 OR rfm = 552 OR rfm = 541 OR rfm = 542 OR rfm = 533 OR rfm = 532 OR
                  rfm = 531 OR rfm = 452 OR rfm = 451 OR rfm = 442 OR rfm = 441 OR rfm = 431 OR rfm = 453 OR
                  rfm = 433 OR rfm = 432 OR rfm = 423 OR rfm = 353 OR rfm = 352 OR rfm = 351 OR rfm = 342 OR
                  rfm = 341 OR rfm = 333 OR rfm = 323
                 THEN 3
             WHEN rfm = 512 OR rfm = 511 OR rfm = 422 OR rfm = 421 OR rfm = 412 OR rfm = 411 OR rfm = 311
                 THEN 4
             WHEN rfm = 525 OR rfm = 524 OR rfm = 523 OR rfm = 522 OR rfm = 521 OR rfm = 515 OR rfm = 514 OR
                  rfm = 513 OR rfm = 425 OR rfm = 424 OR rfm = 413 OR rfm = 414 OR rfm = 415 OR rfm = 315 OR
                  rfm = 314 OR rfm = 313
                 THEN 5
             WHEN rfm = 535 OR rfm = 534 OR rfm = 443 OR rfm = 434 OR rfm = 343 OR rfm = 334 OR rfm = 325 OR rfm = 324
                 THEN 6
             WHEN rfm = 331 OR rfm = 321 OR rfm = 312 OR rfm = 221 OR rfm = 213 OR rfm = 231 OR rfm = 241 OR rfm = 251
                 THEN 7
             WHEN rfm = 255 OR rfm = 254 OR rfm = 245 OR rfm = 244 OR rfm = 253 OR rfm = 252 OR rfm = 243 OR
                  rfm = 242 OR rfm = 235 OR rfm = 234 OR rfm = 225 OR rfm = 224 OR rfm = 153 OR rfm = 152 OR
                  rfm = 145 OR rfm = 143 OR rfm = 142 OR rfm = 135 OR rfm = 134 OR rfm = 133 OR rfm = 125 OR rfm = 124
                 THEN 8
             WHEN rfm = 155 OR rfm = 154 OR rfm = 144 OR rfm = 214 OR rfm = 215 OR rfm = 115 OR rfm = 114 OR rfm = 113
                 THEN 9
             WHEN rfm = 332 OR rfm = 322 OR rfm = 233 OR rfm = 232 OR rfm = 223 OR rfm = 222 OR rfm = 132 OR
                  rfm = 123 OR rfm = 122 OR rfm = 212 OR rfm = 211
                 THEN 10
             WHEN rfm = 111 OR rfm = 112 OR rfm = 121 OR rfm = 131 OR rfm = 141 OR rfm = 151
                 THEN 11
      END AS                                 segment_rank
       , CASE
             WHEN segment_rank = 1
                 THEN 'Champions'
             WHEN segment_rank = 2
                 THEN 'Loyal'
             WHEN segment_rank = 3
                 THEN 'Potential Loyalists'
             WHEN segment_rank = 4
                 THEN 'New Customers'
             WHEN segment_rank = 5
                 THEN 'Promising'
             WHEN segment_rank = 6
                 THEN 'Need Attention'
             WHEN segment_rank = 7
                 THEN 'About to Sleep'
             WHEN segment_rank = 8
                 THEN 'At Risk'
             WHEN segment_rank = 9
                 THEN 'Cannot Lose Them'
             WHEN segment_rank = 10
                 THEN 'Hibernating Customers'
             WHEN segment_rank = 11
                 THEN 'Lost Customers'
      END AS                                 segment_name
       , rfm
       , total_purchasers
      --r
       , recency
       , months_since_most_recent_order
      --f
       , frequency
       , count_of_orders
      --m
       , monetary
       , ltv
      --other measures
       , lifetime_discounts
       , aov
       , lifetime_discounts / nullif(ltv, 0) lifetime_discount_pct
    FROM rfm