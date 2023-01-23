
    
    

select
    user_public_id as unique_field,
    count(*) as n_records

from "dumps"."dev"."segment_users"
where user_public_id is not null
group by user_public_id
having count(*) > 1


