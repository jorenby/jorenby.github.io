-- This rolls SMS/email monthly subscription transactions back into one row. Allows comparing against old report
SELECT
	transaction_id,
	stripe_charge_id,
	subscription_id,
	status,
	plan_id,
	NULL plan_estimate, -- This changes after rollup, so ignore
	transaction_status,
	created_at,
	date_created_at,
	credits_remaining,
	transaction_source,
	memo,
	min(replace(category, 'sms_', '')) category,
	sum(gross_amount_in_usd) gross_amount_in_usd,
	sum(credits_used) credits_used,
	sum(usd_refunded) usd_refunded
FROM
	"dumps"."dev"."finance_monthly_report"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12