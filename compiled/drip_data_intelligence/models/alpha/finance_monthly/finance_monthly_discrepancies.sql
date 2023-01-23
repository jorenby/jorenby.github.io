WITH max_transaction_id AS (
	SELECT
		max(transaction_id) id
	FROM
		"dumps"."dev"."monthly_service_transaction_totals_by_category"
),
max_transaction_date AS (
	SELECT
		DATE_TRUNC('day',
			created_at) transaction_date
	FROM
		max_transaction_id
	LEFT JOIN "dumps"."dev"."transactions" USING (id)
)
SELECT
  *
FROM
  (
    SELECT
      'new' AS TABLE_NAME
    , *
    FROM
      (
        SELECT
          *
        FROM
          "dumps"."dev"."finance_monthly_report_rollup"
        MINUS
        SELECT
          *
        FROM
          "dumps"."dev"."finance_monthly_report_old"
      )
    UNION ALL
    SELECT
      'old' AS TABLE_NAME
    , *
    FROM
      (
        SELECT
          *
        FROM
          "dumps"."dev"."finance_monthly_report_old"
        MINUS
        SELECT
          *
        FROM
          "dumps"."dev"."finance_monthly_report_rollup"
      )
  )
WHERE
  created_at BETWEEN
    '2022-06-01'
    AND
    (SELECT	transaction_date FROM max_transaction_date)
ORDER BY
  transaction_id
, TABLE_NAME