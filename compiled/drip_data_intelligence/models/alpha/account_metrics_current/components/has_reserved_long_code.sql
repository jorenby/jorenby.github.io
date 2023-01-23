SELECT
	ts.account_id AS account_id,
	MIN(tfc.created_at) AS reserved_long_code_at,
	(reserved_long_code_at IS NOT NULL) AS has_reserved_long_code
FROM
	"dumps"."current"."twilio_subaccounts" ts
	LEFT JOIN "dumps"."current"."twilio_from_codes" tfc ON twilio_subaccount_id = ts.id
GROUP BY
	ts.account_id