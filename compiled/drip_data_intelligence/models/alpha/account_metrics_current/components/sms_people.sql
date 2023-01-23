SELECT account_id,
       TRUE as has_sms_people,
       COUNT(*) AS n_sms_people
FROM "dumps"."current"."sms_people"
WHERE status = 'opted_in'
GROUP BY account_id