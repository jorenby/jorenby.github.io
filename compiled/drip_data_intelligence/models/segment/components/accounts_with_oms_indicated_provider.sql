SELECT
    account_id AS drip_account_id,
    LOWER(oms_indicated) AS drip_oms_indicated_provider
FROM
    "dumps"."dev"."account_metrics_current"