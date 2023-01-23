SELECT
    subscription_id AS drip_subscription_id,
    ROUND(current_mrr :: NUMERIC / 100, 2) AS drip_current_mrr
FROM
    public.subscription_metrics_current