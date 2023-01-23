

select * from "dumps"."dev"."active_forms_current"



  -- this filter will only be applied on an incremental run
  where as_of_date > (select max(as_of_date) from "dumps"."dev"."active_forms_snapshot")

