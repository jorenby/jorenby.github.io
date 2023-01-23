SELECT
  mrr_date,
  category,
  SUM(active :: INT) active_count,
  SUM(active_mrr_cents) active_mrr_cents,
  SUM(activated :: INT) activation_count,
  SUM(activated_mrr_cents) activation_mrr_cents,
  SUM(reactivated :: INT) reactivation_count,
  SUM(reactivated_mrr_cents) reactivation_mrr_cents,
  SUM(expanded :: INT) expansion_count,
  SUM(expanded_mrr_cents :: INT) expansion_mrr_cents,
  SUM(contracted :: INT) contraction_count,
  SUM(contracted_mrr_cents :: INT) contraction_mrr_cents,
  SUM(churned :: INT) churn_count,
  SUM(churned_mrr_cents :: INT) churn_mrr_cents
FROM
  "dumps"."dev"."subscription_kpis_daily"
GROUP BY
  mrr_date,
  category