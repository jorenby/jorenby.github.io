WITH active_accounts_ranked_by_date AS (
    SELECT
        id,
        public_id,
        subscription_id,
        DENSE_RANK() over (
            PARTITION BY subscription_id
            ORDER BY
                created_at
        ) AS account_creation_order
    FROM
        "dumps"."dev"."accounts"
),
first_active_account AS (
    SELECT
        id,
        public_id,
        subscription_id
    FROM
        active_accounts_ranked_by_date
    WHERE
        TRUE
        AND account_creation_order = 1
),
owners AS (
    SELECT
        faa.id AS drip_account_id,
        s.user_id AS drip_user_id,
        faa.public_id AS drip_public_account_id,
        s.id AS drip_subscription_id,
        u.email AS email,
        s.token AS drip_subscription_token,
        s.updated_at AS updated_at
    FROM
        "dumps"."dev"."subscriptions" s
        JOIN "dumps"."dev"."users" u
        ON s.user_id = u.id
        JOIN first_active_account faa
        ON faa.subscription_id = s.id
),
paid_statuses AS (
    SELECT
        distinct drip_subscription_id,
        subscription_status AS paid_status,
        status_last_changed_at
    FROM
        "dumps"."dev"."marketing_subscription_status"
    WHERE
        subscription_status IN (
            'active',
            'churned',
            'delinquent',
            'pending_cancellation'
        )
),
subscriptions_post_2020 AS (
    SELECT
        id AS drip_subscription_id
    FROM
        "dumps"."dev"."subscriptions"
    WHERE
        created_at > '2021-01-01'
),
statuses_post_2020 AS (
    SELECT
        distinct drip_subscription_id,
        subscription_status AS post_2020_status,
        status_last_changed_at
    FROM
        "dumps"."dev"."marketing_subscription_status"
        JOIN subscriptions_post_2020 USING (drip_subscription_id)
),
drip_employee_check AS (
    SELECT
        id AS drip_subscription_id,
        primary_email LIKE '%@drip.com'
        OR comped = 't' AS created_by_drip_employee
    FROM
        "dumps"."dev"."subscriptions"
        INNER JOIN (
            SELECT
                id AS user_id,
                email AS primary_email
            FROM
                "dumps"."dev"."users"
        ) USING (user_id)
)
SELECT
    owners.drip_account_id,
    owners.drip_user_id,
    owners.drip_public_account_id,
    owners.drip_subscription_id,
    owners.email,
    owners.drip_subscription_token,
    COALESCE(
        ps.paid_status,
        sp2020.post_2020_status
    ) AS subscription_status,
    COALESCE(
        drec.created_by_drip_employee,
        false
    ) AS created_by_drip_employee,
    owners.updated_at,
    COALESCE(
        ps.status_last_changed_at,
        sp2020.status_last_changed_at
    ) AS status_last_changed_at
FROM
    owners
    LEFT JOIN paid_statuses ps USING (drip_subscription_id)
    LEFT JOIN statuses_post_2020 sp2020 USING (drip_subscription_id)
    LEFT JOIN drip_employee_check drec USING (drip_subscription_id)
WHERE
    TRUE
    AND subscription_status IS NOT NULL