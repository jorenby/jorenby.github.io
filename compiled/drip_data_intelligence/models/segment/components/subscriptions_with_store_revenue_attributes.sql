WITH total_account_attributed_revenue AS (
	SELECT
		account_id AS id
		, SUM(email_attributed_revenue_USD) AS total_email_attributed_revenue_USD
		, SUM(sms_attributed_revenue_USD) AS total_sms_attributed_revenue_USD
		, SUM(total_revenue_USD) AS total_revenue_USD
	FROM "dumps"."dev"."account_attributed_revenue"
	GROUP BY account_id
)
SELECT
	a.subscription_id AS drip_subscription_id
	, SUM(total_email_attributed_revenue_USD) AS drip_revenue_rate_attributed_email
	, SUM(total_sms_attributed_revenue_USD) AS drip_revenue_rate_attributed_sms
	, SUM(total_revenue_USD) AS drip_total_store_revenue
	, (drip_revenue_rate_attributed_email + drip_revenue_rate_attributed_sms > 0) AS drip_has_attributed_revenue
	, drip_total_store_revenue > 0 AS drip_has_revenue
FROM "dumps"."dev"."accounts" a
JOIN total_account_attributed_revenue USING (id)
GROUP BY a.subscription_id