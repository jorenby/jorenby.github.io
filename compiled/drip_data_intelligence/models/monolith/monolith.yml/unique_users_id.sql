
    
    

select
    id as unique_field,
    count(*) as n_records

from "dumps"."dev"."users"
where id is not null
group by id
having count(*) > 1


