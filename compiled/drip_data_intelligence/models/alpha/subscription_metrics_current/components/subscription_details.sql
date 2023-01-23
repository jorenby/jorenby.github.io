SELECT subscription_id,
       activated_at,
       is_active,
       activated_month_cohort,
       activated_week_cohort,
       initial_mrr,
       current_mrr,
       trial_intent,
       activated
FROM "dumps"."current"."subscription_details"