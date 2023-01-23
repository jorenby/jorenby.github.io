
    
    

select
    account_public_id as unique_field,
    count(*) as n_records

from "dumps"."dev"."account_dimensions"
where account_public_id is not null
group by account_public_id
having count(*) > 1


