SELECT account_id
    , count(*)                                            legacy_forms_total
    , sum(CASE WHEN status = 'active' THEN 1 ELSE 0 END)  legacy_forms_active_current
FROM "dumps"."dev"."forms"
GROUP BY account_id