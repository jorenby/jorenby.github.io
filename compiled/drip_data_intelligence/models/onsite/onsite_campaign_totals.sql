SELECT account_id
    , count(*)                                            onsite_campaigns_total
    , sum(CASE WHEN status = 'active' THEN 1 ELSE 0 END)  onsite_campaigns_active_current
FROM "dumps"."current"."sn_forms"
GROUP BY account_id