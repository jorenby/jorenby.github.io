with pre_agg as
    (
        SELECT pds.account_id
             , pds.as_of_date
             , pds.subscription_id
             , pds.account_public_id
             , pds.activation_date
             , pds.activation_cohort_week
             , pds.is_active
             , pds.days_active
             , pds.churn_day
             , pds.mrr_current
             , pds.trust_status
             , pds.is_oms_connected
             , pds.oms_connected_date
             , pds.oms_connected_provider
             , pds.oms_indicated_date
             , pds.is_oms_indicated
             , pds.oms_indicated_provider
             , pds.is_first_person_added
             , pds.first_person_added_date
             , pds.is_first_email_campaign_sent
             , pds.first_email_campaign_sent_date
             , pds.is_js_snippet_installed
             , pds.js_snippet_installed_date
             , pds.is_workflow_enabled
             , pds.first_workflow_enabled_date
             , pds.is_form_enabled
             , pds.first_form_enabled_date
             , pds.is_first_sms_sent
             , pds.first_sent_sms_date
             , pds.last_logged_in_at
             , pds.last_payment_date
             , pds.last_payment_amount
             , pes.subscription_id
             , pes.account_public_id
             , pes.forms_activated_total
             , pes.forms_activated_by_day
             , pes.forms_activated_7_days
             , pes.forms_activated_30_days
             , pes.workflows_activated_total
             , pes.workflows_activated_by_day
             , pes.workflows_activated_7_days
             , pes.workflows_activated_30_days
             , pes.campaigns_sent_total
             , pes.campaigns_sent_by_day
             , pes.campaigns_sent_7_days
             , pes.campaigns_sent_30_days
             , pes.campaigns_delivered_total
             , pes.campaigns_delivered_by_day
             , pes.campaigns_delivered_7_days
             , pes.campaigns_delivered_30_days
             , pes.segments_created_by_day
             , pes.segments_created
             , pes.segments_created_7_days
             , pes.segments_created_30_days
             , dim_date.calendar_date
             , dim_date.week_of_year
             , dim_date.week_name
             , dim_date.day_of_week
             , dim_date.first_date_of_week
             , dim_date.last_date_of_week
             , dim_date.first_date_of_month
             , dim_date.last_date_of_month
             , dim_date.first_date_of_quarter
             , dim_date.last_date_of_quarter
             , dim_date.first_date_of_year
             , dim_date.last_date_of_year
             , dim_date.calendar_month
             , dim_date.calendar_day
             , dim_date.calendar_year
             , dim_date.calendar_quarter
             , dim_date.days_in_month
             , dim_date.day_name
             , dim_date.month_name
             , row_number()
               OVER (
                   PARTITION BY pds.account_id, dim_date.first_date_of_week
                   ORDER BY pds.as_of_date DESC) AS n
          FROM alpha.product_usage_dimensions_snapshot pds
                   JOIN alpha.product_usage_events_snapshot pes USING (account_id, as_of_date)
                   JOIN "dumps"."alpha"."dim_date" ON pds.as_of_date = dim_date.calendar_date
    )
SELECT pre_agg.first_date_of_week
     , pre_agg.week_of_year
     , pre_agg.activation_cohort_week
     , pre_agg.is_active
     , count(*)                               AS count_of_accounts
     , sum(
        CASE
            WHEN pre_agg.mrr_current > 0
                THEN pre_agg.mrr_current::double precision
                ELSE pre_agg.last_payment_amount
            END)                              AS total_mrr_with_churn
     , sum(pre_agg.mrr_current)               AS total_active_mrr
     , avg(pre_agg.mrr_current)               AS average_mrr
     , sum(
        CASE
            WHEN pre_agg.is_oms_connected
                THEN 1
                ELSE 0
            END)                              AS oms_connected_count
     , sum(
        CASE
            WHEN pre_agg.is_oms_indicated
                THEN 1
                ELSE 0
            END)                              AS oms_indicated_count
     , sum(
        CASE
            WHEN pre_agg.is_first_person_added
                THEN 1
                ELSE 0
            END)                              AS first_person_added_count
     , sum(
        CASE
            WHEN pre_agg.is_first_email_campaign_sent
                THEN 1
                ELSE 0
            END)                              AS first_email_campaign_sent_count
     , sum(
        CASE
            WHEN pre_agg.is_js_snippet_installed
                THEN 1
                ELSE 0
            END)                              AS js_snippet_installed_count
     , sum(
        CASE
            WHEN pre_agg.is_workflow_enabled
                THEN 1
                ELSE 0
            END)                              AS workflow_enabled_count
     , sum(
        CASE
            WHEN pre_agg.is_form_enabled
                THEN 1
                ELSE 0
            END)                              AS form_enabled_count
     , sum(
        CASE
            WHEN pre_agg.is_first_sms_sent
                THEN 1
                ELSE 0
            END)                              AS first_sms_sent_count
     , sum(pre_agg.forms_activated_total)     AS forms_activated_total
     , avg(pre_agg.forms_activated_total)     AS forms_activated_avg
     , sum(pre_agg.workflows_activated_total) AS workflows_activated_total
     , avg(pre_agg.workflows_activated_total) AS workflows_activated_avg
     , sum(pre_agg.campaigns_sent_total)      AS campaigns_sent_total
     , avg(pre_agg.campaigns_sent_total)      AS campaigns_sent_avg
     , sum(pre_agg.campaigns_delivered_total) AS campaigns_delivered_total
     , avg(pre_agg.campaigns_delivered_total) AS campaigns_delivered_avg
     , sum(pre_agg.segments_created)          AS segments_created_total
     , avg(pre_agg.segments_created)          AS segments_created_avg
  FROM pre_agg
 WHERE pre_agg.n = 1
   AND date_trunc('y'::text, pre_agg.activation_cohort_week::timestamp WITHOUT TIME ZONE) =
       '2022-01-01 00:00:00'::timestamp WITHOUT TIME ZONE
 GROUP BY pre_agg.first_date_of_week, pre_agg.week_of_year, pre_agg.activation_cohort_week, pre_agg.is_active
 ORDER BY pre_agg.first_date_of_week, pre_agg.activation_cohort_week, pre_agg.is_active