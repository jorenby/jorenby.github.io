SELECT
    account_id AS drip_account_id,
    occurred_at :: DATE AS drip_registration_date
FROM
    "dumps"."dev"."account_events"
WHERE
    TRUE
    AND action = 'Registration completed'