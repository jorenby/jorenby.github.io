WITH active_accounts AS (
	SELECT DISTINCT
		account_id
		, account_public_id
		, activated_at
		, current_mrr
	FROM
		"dumps"."dev"."subscription_metrics_current"
	JOIN "dumps"."dev"."account_metrics_current" USING (subscription_id)
	WHERE TRUE
		AND is_active
		AND NOT created_by_drip_employee
),
sn_forms AS (
	SELECT
		account_id
		, form_type
		, COUNT(*) AS forms_count
	FROM
		current.sn_forms
	WHERE TRUE
		AND status = 'active'
	GROUP BY
		account_id,
		form_type
),
drip_forms AS (
	SELECT
		account_id
		, 'drip' AS form_type
		, COUNT(*) AS forms_count
	FROM
		"dumps"."dev"."forms"
	WHERE TRUE
		AND status = 'active'
	GROUP BY
		account_id
),
forms AS (
	SELECT
		*
	FROM
		sn_forms
	UNION ALL
	SELECT
		*
	FROM
		drip_forms
)
SELECT
	account_id
	, SYSDATE::DATE AS as_of_date
	, form_type
	, COALESCE(FORMS_COUNT, 0) AS forms_count
FROM
	active_accounts
LEFT JOIN forms USING (account_id)