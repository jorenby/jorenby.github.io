-- NOTE: this is equivalent to `current.account_dimensions`
--   We update that data in the `account_dimensions` job using this table
--   to ensure old queries keep working
WITH most_recent_subscription_metrics AS (
    SELECT
        subscription_id
        , emic
        , is_active
    FROM (
            SELECT
                subscription_id
                , is_active
                , coalesce(emic, 0) AS emic
                , row_number() OVER (
                    PARTITION BY
                        subscription_id
                    ORDER BY metric_date DESC
                ) AS row_num
            FROM current.product_metrics
            WHERE is_active IS NOT NULL
        ) AS metric_rows
    WHERE row_num = 1
)

, most_recent_account_metrics AS (
    SELECT
        account_id
        , billable_subscribers_v2
    FROM (
            SELECT
                account_daily_usage_stats.account_id
                , account_daily_usage_stats.billable_subscribers_v2
                , row_number() OVER (
                    PARTITION BY
                        account_daily_usage_stats.account_id
                    ORDER BY account_daily_usage_stats.occurred_on DESC
                ) AS row_num
            FROM current.account_daily_usage_stats
        ) AS metric_rows
    WHERE row_num = 1
)

, account_counts AS (
    SELECT
        subscription_id
        , count(*) AS n_account
    FROM "dumps"."dev"."accounts"
    GROUP BY subscription_id
)

, account_weights AS (
    SELECT
        accounts.public_id
        , accounts.subscription_id
        , CASE
            WHEN (sum(
                most_recent_account_metrics.billable_subscribers_v2
                ) OVER (PARTITION BY accounts.subscription_id)
                > 0)
                THEN
                cast(
                    most_recent_account_metrics.billable_subscribers_v2 AS float
                )
                / sum(
                    most_recent_account_metrics.billable_subscribers_v2
                ) OVER (
                    PARTITION BY accounts.subscription_id
                )
            ELSE 1 / cast(account_counts.n_account AS float) END AS pct_account_subscribers
    FROM "dumps"."dev"."accounts"
        INNER JOIN account_counts
            ON
                account_counts.subscription_id = accounts.subscription_id
        LEFT OUTER JOIN most_recent_account_metrics
            ON accounts.id = most_recent_account_metrics.account_id
)

, weighted_mrr AS (
    SELECT
        accounts.public_id
        , most_recent_subscription_metrics.is_active AS is_paying
        , (
            most_recent_subscription_metrics.emic * account_weights.pct_account_subscribers * 12
        )
        / 100 AS arr_usd
    FROM "dumps"."dev"."accounts"
        INNER JOIN most_recent_subscription_metrics
            ON
                accounts.subscription_id = most_recent_subscription_metrics.subscription_id
        LEFT OUTER JOIN account_weights
            ON accounts.public_id = account_weights.public_id
)

, sku_count AS (
    SELECT
        account_id AS account_public_id
        , count(*) AS sku_count
    FROM current.products
    GROUP BY account_id
)

, average_order_value AS (
    SELECT
        p.account_id
        , count(p.id) AS num_purchases
        , avg((p.amount * currency.value) / 100) AS average_order_value_usd
    FROM "dumps"."dev"."purchases" AS p
        INNER JOIN current.currency ON p.currency_code = currency.currency
    WHERE p.canceled_at IS NULL
        AND (
            p.financial_state NOT IN ( 'voided', 'refunded' )
            OR p.financial_state IS NULL
        )
    GROUP BY p.account_id
)

, accounts_with_orders AS (
    SELECT account_id
    FROM "dumps"."dev"."purchases"
    GROUP BY account_id
)
SELECT DISTINCT
    accounts.public_id AS account_public_id
    , account_segments.segment
    , sku_count.sku_count
    , average_order_value.average_order_value_usd AS aov_usd
    , CASE
        WHEN (accounts_with_orders.account_id IS NOT NULL
            AND (account_technology_usages_current.ecomm_platform IS NULL
                OR account_technology_usages_current.ecomm_platform = 'None'))
            THEN 'other'
        WHEN (account_technology_usages_current.ecomm_platform = 'None')
            THEN NULL
        ELSE account_technology_usages_current.ecomm_platform
    END AS ecommerce_platform
    , CASE
        WHEN subscription_dimensions.sales_agency_sourced
            THEN 'agency'
        WHEN subscription_dimensions.sales_sourced
            THEN 'sales'
        ELSE 'self-serve'
    END AS acquisition_source
    , coalesce( weighted_mrr.arr_usd, 0 ) AS arr_usd
    , coalesce( weighted_mrr.is_paying, FALSE ) AS is_paying
    , CASE
        WHEN (weighted_mrr.arr_usd >= 50000)
            THEN 'enterprise'
        WHEN (weighted_mrr.arr_usd >= 12000)
            THEN 'mid_market'
        ELSE 'small_business'
    END AS arr_bucket
    , 
    

    
        CURRENT_DATE
    
 AS etl_date
FROM "dumps"."dev"."accounts"
    LEFT OUTER JOIN current.account_segments
        ON accounts.public_id = account_segments.account_public_id
    LEFT OUTER JOIN "dumps"."dev"."account_technology_usages_current"
        ON account_technology_usages_current.account_public_id = accounts.public_id
    LEFT OUTER JOIN current.subscription_dimensions
        ON subscription_dimensions.subscription_id = accounts.subscription_id
    LEFT OUTER JOIN weighted_mrr
        ON weighted_mrr.public_id = accounts.public_id
    LEFT OUTER JOIN sku_count
        ON sku_count.account_public_id = accounts.public_id
    LEFT OUTER JOIN average_order_value
        ON average_order_value.account_id = accounts.id
    LEFT OUTER JOIN accounts_with_orders
        ON accounts_with_orders.account_id = accounts.id