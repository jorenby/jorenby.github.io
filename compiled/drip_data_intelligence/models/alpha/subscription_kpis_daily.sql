WITH active_and_previous_mrr AS (
  SELECT
    subscription_id,
    mrr_date,
    lag(current_mrr_cents, 1) OVER (
      PARTITION BY subscription_id
      ORDER BY
        mrr_date ASC
    ) AS previous_mrr_cents_raw,
    ISNULL(previous_mrr_cents_raw, 0) AS previous_mrr_cents,
    current_mrr_cents,
    category
  FROM
    "dumps"."dev"."subscription_mrr_daily"
),
subscription_activation_counts AS (
	SELECT
    subscription_id,
    mrr_date,
    (
      current_mrr_cents > 0 AND
      previous_mrr_cents = 0
    ) AS newly_activated,
    SUM(newly_activated :: INT) OVER (
      PARTITION BY subscription_id
      ORDER BY mrr_date ROWS UNBOUNDED PRECEDING) AS subscription_activation_count
    FROM active_and_previous_mrr
)
SELECT
  subscription_id,
  mrr_date,
  current_mrr_cents,
  previous_mrr_cents,
  (current_mrr_cents > 0) AS active,
  current_mrr_cents AS active_mrr_cents,
  (
    newly_activated AND
    subscription_activation_count <= 1
  ) AS activated,
  (current_mrr_cents * activated :: INT) AS activated_mrr_cents,
  (newly_activated AND
    subscription_activation_count > 1 ) AS reactivated,
  (current_mrr_cents * reactivated :: INT) AS reactivated_mrr_cents,
  (
    previous_mrr_cents < current_mrr_cents
    AND previous_mrr_cents <> 0
  ) AS expanded,
  (
    (current_mrr_cents - previous_mrr_cents) * expanded :: INT
  ) AS expanded_mrr_cents,
  (
    previous_mrr_cents > current_mrr_cents
    AND current_mrr_cents <> 0
  ) AS contracted,
  (
    (previous_mrr_cents - current_mrr_cents) * contracted :: INT
  ) AS contracted_mrr_cents,
  (
    current_mrr_cents = 0
    AND previous_mrr_cents > 0
  ) AS churned,
  (previous_mrr_cents * churned :: INT) AS churned_mrr_cents,
  category
FROM
  active_and_previous_mrr
    LEFT JOIN subscription_activation_counts
    USING (subscription_id, mrr_date)