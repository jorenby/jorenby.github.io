WITH accounts_with_api_purchase AS (
    SELECT
        distinct account_id AS id
        , 1 AS has_api_purchase
    FROM "dumps"."dev"."purchases"
    WHERE TRUE
        AND source = 'shopper_activity'
)
SELECT
	acc.id AS drip_account_id
	, CASE WHEN has_api_purchase = 1 THEN TRUE ELSE FALSE END AS drip_has_api_purchase
FROM "dumps"."dev"."accounts" acc
LEFT JOIN accounts_with_api_purchase USING (id)