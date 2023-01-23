SELECT
    account_id AS drip_account_id,
    action
FROM
    "dumps"."dev"."account_events"
WHERE
    1 = 1
    and action IN (
        'Abandoned browse workflow activated',
        'Abandoned cart workflow activated',
        'Post first purchase workflow activated',
        'Post repeat purchase workflow activated',
        'Post-purchase workflow activated',
        'Welcome workflow activated',
        'Win-back workflow activated'
    )