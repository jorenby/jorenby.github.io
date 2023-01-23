SELECT DISTINCT -- TODO: Kludge. Figure out why we're getting duplicates.
    subject_id AS drip_subscription_id,
    final_status AS subscription_status,
    status_last_changed_at
FROM
    "dumps"."dev"."marketing_subscription_status_audit"