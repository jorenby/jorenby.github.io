
    
    

select
    subscription_id as unique_field,
    count(*) as n_records

from "dumps"."current"."subscription_details"
where subscription_id is not null
group by subscription_id
having count(*) > 1


