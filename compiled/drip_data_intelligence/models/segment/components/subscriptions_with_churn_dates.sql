with subscription_dates as
         (
             select distinct
                    id,
                    cancelled_at,
                    pending_cancellation_at
               from "dumps"."dev"."subscriptions"
         ),
     delinquent_date as
         (
             select id,
                    created_at drip_delinquent_date
               from data_intelligence.subscription_state_transitions
              where "to" = 'delinquent'
                and subject_type = 'Subscription'
                and "from" = 'active'
         )
select id drip_subscription_id,
       cancelled_at drip_churn_date,
       pending_cancellation_at drip_pending_cancellation_date,
       drip_delinquent_date
  from subscription_dates
  full outer join delinquent_date using (id)
where cancelled_at is not null or pending_cancellation_at is not null or drip_delinquent_date is not null