SELECT
f.account_id AS account_id
, TRUE AS has_sms_optin_form
, MIN(cf.created_at) AS created_sms_option_form_at
FROM "dumps"."dev"."forms" AS f
	JOIN "dumps"."current"."custom_fields" cf
	ON f.id = cf.form_id
WHERE 1 = 1
AND cf.data_type = 'phone'
AND cf.is_consent_asked = TRUE
GROUP BY 1, 2