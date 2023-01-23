
    
    

select
    account_id as unique_field,
    count(*) as n_records

from "dumps"."dev"."onsite_campaign_totals"
where account_id is not null
group by account_id
having count(*) > 1


