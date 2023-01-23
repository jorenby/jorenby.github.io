WITH account_technology_usages_with_row_num AS (
        SELECT account_public_id
             , ecomm_platform
             , updated_at
             , row_number() OVER (
                PARTITION BY account_public_id
                ORDER BY updated_at DESC
             )                                  AS row_num
        FROM current.account_technology_usages
  )
SELECT account_public_id, ecomm_platform, updated_at
  FROM account_technology_usages_with_row_num
 WHERE row_num = 1