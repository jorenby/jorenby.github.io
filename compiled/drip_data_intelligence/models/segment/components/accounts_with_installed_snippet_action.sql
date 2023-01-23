SELECT
    account_id AS drip_account_id,
    action
FROM
    "dumps"."dev"."account_events"
WHERE
    1 = 1
    and action ILIKE 'Installed Snippet'