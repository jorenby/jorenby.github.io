SELECT
  a.account_id,
  p.account_id IS NOT NULL as has_purchases
FROM
  (
    SELECT
      id AS account_id
    FROM
      "dumps"."dev"."accounts"
  ) a
  LEFT JOIN (
    SELECT
      DISTINCT account_id
    FROM
      "dumps"."dev"."purchases"
  ) p USING (account_id)