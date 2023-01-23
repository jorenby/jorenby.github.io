WITH monthly_totals AS (
  SELECT
    date_trunc('MONTH', mrr_date) AS first_of_month_date,
    category,
    SUM(expansion_count) AS expansion_count_by_month,
    SUM(contraction_count) AS contraction_count_by_month
  FROM
    "dumps"."dev"."total_kpis_daily"
  GROUP BY
    first_of_month_date,
    category
)
SELECT
  "mrr_date",
  "active_count",
  "active_mrr_cents",
  "activation_count",
  "activation_mrr_cents",
  "reactivation_count",
  "reactivation_mrr_cents",
  "expansion_count",
  "expansion_mrr_cents",
  "contraction_count",
  "contraction_mrr_cents",
  "churn_count",
  "churn_mrr_cents",
  monthly_totals.*
FROM
  "dumps"."dev"."total_kpis_daily"
  LEFT JOIN monthly_totals
    ON date_trunc('MONTH', "dumps"."dev"."total_kpis_daily".mrr_date) = monthly_totals.first_of_month_date
    AND "dumps"."dev"."total_kpis_daily".category = monthly_totals.category
ORDER BY
  mrr_date ASC