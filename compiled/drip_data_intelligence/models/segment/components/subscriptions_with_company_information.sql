WITH active_accounts_ranked_by_date AS (
  SELECT
    subscription_id,
    name,
    url,
    DENSE_RANK() OVER (
      PARTITION BY subscription_id
      ORDER BY
        created_at
    ) AS account_creation_order
  FROM
    "dumps"."dev"."accounts"
),
first_active_account_with_information AS (
  SELECT
    subscription_id,
    name as company,
    url as website
  FROM
    active_accounts_ranked_by_date
  WHERE
    TRUE
    AND account_creation_order = 1
),
users_with_information AS (
  SELECT
    id AS user_id,
    name,
    split_part(name, ' ', 1) AS firstname,
    split_part(name, ' ', 2) AS lastname
  FROM
    "dumps"."dev"."users"
),
final AS (
  SELECT
    s.id AS drip_subscription_id,
    name,
    firstname,
    lastname,
    company,
    website
  FROM
    "dumps"."dev"."subscriptions" s
    JOIN users_with_information USING (user_id)
    JOIN first_active_account_with_information faa ON s.id = faa.subscription_id
)
SELECT
  drip_subscription_id,
  firstname,
  lastname,
  company,
  website
FROM
  final