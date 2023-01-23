SELECT
    account_id AS drip_account_id,
    has_sent_sms AS drip_sent_first_sms
FROM
    "dumps"."dev"."account_metrics_current"