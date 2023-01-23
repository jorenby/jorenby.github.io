SELECT account_id,
       TRUE AS has_at_least_300_sms_people,
       sms_person_added_at AS recieved_300_sms_people_at
       FROM
(SELECT account_id,
       ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY created_at) AS row_num,
       created_at AS sms_person_added_at
FROM "dumps"."current"."sms_people")
WHERE row_num = 300