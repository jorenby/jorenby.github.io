WITH purchases_truncated_to_day AS (
	SELECT
		account_id
		, amount_usd
		, date_trunc('d',occurred_at)::DATE as occurred_at
		, attributed_email_id
		, attributed_smsable_id
		, currency_code AS purchase_currency
	FROM "dumps"."dev"."purchases_corrected")
SELECT
    account_id
    , occurred_at
    , SUM(
        CASE
            WHEN attributed_email_id IS NOT NULL THEN amount_usd
            ELSE 0
        END
    ) AS email_attributed_revenue_USD
    , SUM(
        CASE
            WHEN attributed_smsable_id IS NOT NULL THEN amount_usd
            ELSE 0
        END
    ) AS sms_attributed_revenue_USD
    , SUM (amount_usd) AS total_revenue_USD
FROM purchases_truncated_to_day
GROUP BY account_id, occurred_at
ORDER BY account_id, occurred_at