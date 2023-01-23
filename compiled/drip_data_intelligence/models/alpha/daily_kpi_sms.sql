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
  "first_of_month_date",
  "expansion_count_by_month",
  "contraction_count_by_month"
FROM
  "dumps"."dev"."daily_kpi_by_category"
WHERE
  category = 'sms_monthly_service'
ORDER BY mrr_date ASC