SELECT account_id,
       MIN(created_at) AS first_sms_person_at
FROM "dumps"."current"."sms_people"
GROUP BY account_id