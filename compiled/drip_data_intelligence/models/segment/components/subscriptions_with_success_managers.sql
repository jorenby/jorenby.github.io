SELECT
  s.id AS drip_subscription_id,
  CASE
    WHEN account_manager_id IS NULL THEN FALSE
    ELSE TRUE
  END AS drip_success_manager,
  COALESCE(a.name, 'none') AS drip_success_manager_name
FROM
  "dumps"."dev"."subscriptions" s
  LEFT JOIN current.admins a
  ON s.account_manager_id = a.id