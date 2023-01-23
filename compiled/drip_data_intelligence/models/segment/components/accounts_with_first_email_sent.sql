SELECT
    account_id AS drip_account_id,
    action
FROM
    current.delivery_events
WHERE
    1 = 1
    AND action = 'deliver'
    AND year >= 2021
GROUP BY
    account_id,
    action