WITH setting_up_subscriptions AS (
    SELECT
        id AS subject_id,
        NULL AS "from",
        'setting_up' AS "to",
        created_at
    FROM
        "dumps"."dev"."subscriptions"
    WHERE
        TRUE
        AND status = 'setting_up'
),
state_transitions_combined AS (
    SELECT
        subject_id,
        "from",
        "to",
        created_at
    FROM
        data_intelligence.subscription_state_transitions
    WHERE
        TRUE
        AND "to" NOT LIKE 'trust%'
        AND "from" NOT LIKE 'trust%'
    UNION
    ALL
    SELECT
        *
    FROM
        setting_up_subscriptions
),
state_transitions_ranked AS (
    SELECT
        subject_id,
        "to",
        "from",
        created_at,
        DENSE_RANK() OVER (
            PARTITION BY subject_id
            ORDER BY
                created_at DESC
        ) AS state_transitions_order
    FROM
        state_transitions_combined
),
trust_statuses_ranked AS (
    SELECT
        subject_id,
        "to",
        "from",
        created_at,
        DENSE_RANK() OVER (
            PARTITION BY subject_id
            ORDER BY
                created_at DESC
        ) AS trust_status_order
    FROM
        data_intelligence.subscription_state_transitions
    WHERE
        TRUE
        AND "to" LIKE 'trust%'
        AND "from" LIKE 'trust%'
),
last_state_transition AS (
    SELECT
        subject_id,
        "from" AS last_status_from,
        "to" AS last_status_to,
        created_at
    FROM
        state_transitions_ranked
    WHERE
        TRUE
        AND state_transitions_order = 1
),
last_trust_transition AS (
    SELECT
        subject_id,
        created_at,
        "from" AS last_trust_status_from,
        "to" AS last_trust_status_to
    FROM
        trust_statuses_ranked
    WHERE
        TRUE
        AND trust_status_order = 1
),
days_between_penultimate_and_ultimate_state_transition AS (
    SELECT
        subject_id,
        DATEDIFF(
            'day',
            penultimate.created_at,
            ultimate.created_at
        ) AS days_between_state_change
    FROM
        last_state_transition AS ultimate
        JOIN (
            SELECT
                subject_id,
                "from" AS last_status_from,
                "to" AS last_status_to,
                created_at
            FROM
                state_transitions_ranked
            WHERE
                TRUE
                AND state_transitions_order = 2
        ) AS penultimate USING (subject_id)
),
days_between_penultimate_and_ultimate_trust_transition AS (
    SELECT
        subject_id,
        DATEDIFF(
            'day',
            penultimate.created_at,
            ultimate.created_at
        ) AS days_between_trust_change
    FROM
        last_trust_transition AS ultimate
        JOIN (
            SELECT
                subject_id,
                "from" AS last_status_from,
                "to" AS last_status_to,
                created_at
            FROM
                trust_statuses_ranked
            WHERE
                TRUE
                AND trust_status_order = 2
        ) AS penultimate USING (subject_id)
)
SELECT
    subject_id,
    last_status_from,
    last_status_to,
    lst.created_at as status_last_changed_at,
    last_trust_status_from,
    last_trust_status_to,
    days_between_state_change,
    days_between_trust_change,
    CASE
        WHEN (
            last_trust_status_to = 'trust.banned'
            OR last_trust_status_to = 'trust.blocked'
        )
        AND days_between_trust_change NOT IN (89, 90, 91) THEN -- Check for 90 day block/ban
        'banned'
        ELSE NULL
    END AS is_banned_or_blocked -- Anyone legitimately banned or blocked should have a status of 'banned' (COALESCE BELOW)
,
    CASE
        WHEN last_status_from = 'trial_expired'
        AND last_status_to = 'cancelled'
        AND days_between_state_change IN(13, 14, 15) THEN -- check for 14 day auto cancellation
        'trial_expired'
        ELSE NULL
    END AS had_trial_expire -- If they aren't banned and the last legitimate status was trial_expired, they are trial expired (COALESCE BELOW)
,
    CASE
        WHEN last_status_to = 'cancelled' THEN 'churned'
        ELSE last_status_to
    END AS cancelled_to_churned,
    COALESCE(
        is_banned_or_blocked,
        had_trial_expire,
        cancelled_to_churned
    ) AS final_status
FROM
    last_state_transition lst
    LEFT JOIN last_trust_transition ltt USING (subject_id)
    LEFT JOIN days_between_penultimate_and_ultimate_state_transition USING (subject_id)
    LEFT JOIN days_between_penultimate_and_ultimate_trust_transition USING (subject_id)