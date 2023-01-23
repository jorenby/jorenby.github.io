
    
    

select
    subscription_id as unique_field,
    count(*) as n_records

from "dumps"."dev"."subscription_metrics_current"
where subscription_id is not null
group by subscription_id
having count(*) > 1


