



SELECT account_id, occurred_at, action, email_id, delivery_id, subscriber_id, emailable_type
FROM  "dumps"."current"."delivery_events"


WHERE
((year = '2023' AND month = '1' AND day = '22')
OR (year = '2023' AND month = '1' AND day = '23'))
AND occurred_at > (select max(occurred_at) from "dumps"."dev"."delivery_events")

