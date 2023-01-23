SELECT id                                                                                        AS subscription_id,
       CASE
           WHEN type = 'activated_emic' THEN 'converted'
           ELSE CASE
                    WHEN type = 'expanded_emic' THEN 'expanded'
                    ELSE CASE
                             WHEN type = 'contracted_emic' THEN 'contracted'
                             ELSE CASE WHEN type = 'churned_emic' THEN 'churned'
                                ELSE CASE WHEN type = 'reactivated_emic' THEN 'reconverted' END END END END END AS action,
       occurred_at,
       seq                                                                                       AS sequence,
       amount                                                                                    AS mrr_cents_delta,
       new_value                                                                                 AS mrr_cents_current
FROM "dumps"."finance"."mrr_kpi"