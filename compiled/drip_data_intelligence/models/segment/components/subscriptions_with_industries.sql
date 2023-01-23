WITH industries_ranked AS (
    SELECT
        aic.created_at
        , subs.id AS drip_subscription_id
        , subs.user_id
        , ic.display_name AS drip_manually_enriched_industry
        , sc.display_name AS drip_manually_enriched_subindustry
        , ROW_NUMBER() OVER (PARTITION BY drip_subscription_id ORDER BY aic.created_at DESC) AS industry_ranked_by_time_desc
    FROM current.account_industry_categories aic
    JOIN current.industry_categories ic
        ON aic.industry_category_id = ic.id
    JOIN current.industry_subcategories sc
        ON aic.industry_category_id = sc.id
    RIGHT JOIN "dumps"."dev"."subscriptions" subs
        ON subs.user_id = aic.user_id
    WHERE TRUE
        AND ic.display_name IS NOT NULL
)
SELECT
    drip_subscription_id
    , user_id
    , drip_manually_enriched_industry
    , drip_manually_enriched_subindustry
FROM industries_ranked
WHERE TRUE
    AND industry_ranked_by_time_desc = 1