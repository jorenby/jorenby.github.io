SELECT DISTINCT
      users.public_id                                              user_public_id
    , users.public_id                                              id
    , users.name
    , users.first_name
    , users.last_name
    , users.created_at
    , users.email
    , ('https://www.getdrip.com/faucet/users/' || users.public_id) user_faucet_link
FROM "dumps"."dev"."segment_user_group_mapping"
      LEFT JOIN "dumps"."dev"."users" ON segment_user_group_mapping.user_public_id = users.public_id