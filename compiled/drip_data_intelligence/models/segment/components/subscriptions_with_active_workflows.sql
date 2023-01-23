SELECT
  subscription_id as drip_subscription_id,
  count(*) AS drip_active_workflows
FROM
  "dumps"."dev"."workflows" w
  JOIN "dumps"."dev"."accounts" a
  ON w.account_id = a.id
WHERE
  TRUE
  AND w.status = 'active'
GROUP BY
  subscription_id