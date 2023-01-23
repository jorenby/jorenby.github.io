SELECT a.public_id account_public_id
     , u.public_id user_public_id
  FROM "dumps"."dev"."segment_filtered_users_recently_not_churned" sfurnc
      LEFT JOIN "dumps"."dev"."accounts" a ON sfurnc.drip_subscription_id = a.subscription_id
      LEFT JOIN "dumps"."dev"."memberships" m ON a.id = m.account_id
      LEFT JOIN "dumps"."dev"."users" u ON m.user_id = u.id
 WHERE u.public_id IS NOT NULL
   AND m.status = 'active'