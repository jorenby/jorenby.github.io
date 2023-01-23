SELECT
  s.id AS drip_subscription_id,
  count(
    distinct m.account_id
  ) AS drip_users_count
FROM
  "dumps"."dev"."subscriptions" s
  LEFT JOIN "dumps"."dev"."memberships" m
  ON s.user_id = m.user_id
GROUP BY
  s.id