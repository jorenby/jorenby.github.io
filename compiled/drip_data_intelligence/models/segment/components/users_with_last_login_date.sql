SELECT
    id as drip_user_id,
    DATE_TRUNC('day', last_logged_in_at) as drip_last_login_date
FROM
    "dumps"."dev"."users"