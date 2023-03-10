SELECT pds.subscription_id
       , pds.account_id
       , pds.account_public_id
       , pds.as_of_date
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
    FROM alpha.product_usage_dimensions_snapshot pds
             JOIN alpha.product_usage_events_snapshot pes USING (account_id, as_of_date)
   WHERE NOT pds.is_active
     AND extract('y' from as_of_date) = 2022