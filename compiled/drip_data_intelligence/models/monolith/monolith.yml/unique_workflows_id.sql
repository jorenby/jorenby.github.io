
    
    

select
    id as unique_field,
    count(*) as n_records

from "dumps"."dev"."workflows"
where id is not null
group by id
having count(*) > 1


