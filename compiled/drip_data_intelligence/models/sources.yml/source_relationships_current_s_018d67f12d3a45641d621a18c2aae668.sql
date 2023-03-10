
    
    

with child as (
    select subscription_id as from_field
    from "dumps"."current"."subscription_details"
    where subscription_id is not null
),

parent as (
    select id as to_field
    from "dumps"."current"."subscriptions"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


