SELECT
    DISTINCT segment_filtered_users_recently_not_churned.drip_account_id,
    segment_filtered_users_recently_not_churned.drip_user_id,
    segment_filtered_users_recently_not_churned.drip_public_account_id,
    segment_filtered_users_recently_not_churned.drip_subscription_id,
    segment_filtered_users_recently_not_churned.email,
    segment_filtered_users_recently_not_churned.drip_subscription_token,
    CASE
        WHEN accounts_with_first_email_sent.action IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS drip_sent_first_email,
    CASE
        WHEN accounts_with_first_sms_sent.drip_sent_first_sms IS NOT NULL THEN accounts_with_first_sms_sent.drip_sent_first_sms
        ELSE FALSE
    END AS drip_sent_first_sms,
    accounts_with_legacy_form_activated_events.has_legacy_form_activated_event AS drip_form_enabled,
    CASE
        WHEN accounts_with_oms_connected_provider.drip_oms_connected_provider IS NOT NULL THEN TRUE
        ELSE FALSE
    END as drip_is_oms_connected,
    accounts_with_oms_connected_provider.drip_oms_connected_provider,
    CASE
        WHEN accounts_with_oms_indicated_provider.drip_oms_indicated_provider IS NOT NULL THEN TRUE
        ELSE FALSE
    END as drip_is_oms_indicated,
    accounts_with_oms_indicated_provider.drip_oms_indicated_provider,
    CASE
        WHEN accounts_with_first_person_added.action IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS drip_people_added,
    CASE
        WHEN accounts_with_installed_snippet_action.action IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS drip_added_javascript,
    subscriptions_with_trial_start_at.drip_trial_start_date,
    subscriptions_with_trial_end_date.drip_trial_end_date,
    subscriptions_with_trust_status.drip_trust_status,
    CASE
        WHEN accounts_with_workflow_enabled.action IS NOT NULL THEN TRUE
        ELSE FALSE
    END as drip_workflow_enabled,
    num_accounts_connected_to_subscription.drip_accounts_connected_subscription,
    subscriptions_with_last_payments.drip_last_payment_date,
    subscriptions_with_activation_dates.drip_activation_date,
    subscriptions_with_activation_dates.drip_activation_date_cohort_week,
    subscriptions_with_trial_weeks.drip_trial_cohort_week,
    segment_filtered_users_recently_not_churned.subscription_status,
    accounts_with_first_registration_date.drip_registration_date,
    accounts_with_email_campaigns_sent.drip_email_campaigns_sent_all_time,
    accounts_with_email_campaigns_sent.drip_email_campaigns_sent_last_30_days,
    accounts_with_email_campaigns_sent.drip_email_campaigns_sent_last_7_days,
    
    

    
        CURRENT_DATE
    
 AS drip_cron_last_updated,
    accounts_with_sms_campaigns_sent.drip_sms_campaigns_sent_all_time,
    accounts_with_sms_campaigns_sent.drip_sms_campaigns_sent_last_30_days,
    accounts_with_sms_campaigns_sent.drip_sms_campaigns_sent_last_7_days,
    accounts_with_api_purchases.drip_has_api_purchase,
    accounts_with_has_ever_connected_oms.drip_has_ever_connected_oms,
    subscriptions_with_pricing_amounts.drip_pricing_amount,
    subscriptions_with_current_mrr.drip_current_mrr,
    subscriptions_with_financial_statuses.paid_in_the_last_30_days,
    subscriptions_with_financial_statuses.last_payment_amount_greater_than_0,
    subscriptions_with_list_size.drip_email_list_size,
    subscriptions_with_company_information.firstname,
    subscriptions_with_company_information.lastname,
    subscriptions_with_company_information.company,
    subscriptions_with_company_information.website,
    subscriptions_with_active_workflows.drip_active_workflows,
    subscriptions_with_sku_count.drip_sku_count,
    subscriptions_with_success_managers.drip_success_manager,
    subscriptions_with_success_managers.drip_success_manager_name,
    subscriptions_with_user_counts.drip_users_count,
    subscriptions_with_sms_subscribers_count.drip_sms_subscriber_count,
    subscriptions_with_industries.drip_manually_enriched_industry,
    subscriptions_with_industries.drip_manually_enriched_subindustry,
    subscriptions_with_churn_dates.drip_churn_date,
    subscriptions_with_churn_dates.drip_pending_cancellation_date,
    subscriptions_with_churn_dates.drip_delinquent_date,
    subscriptions_with_store_revenue_attributes.drip_revenue_rate_attributed_sms,
    subscriptions_with_store_revenue_attributes.drip_revenue_rate_attributed_email,
    subscriptions_with_store_revenue_attributes.drip_total_store_revenue,
    subscriptions_with_store_revenue_attributes.drip_has_attributed_revenue,
    subscriptions_with_store_revenue_attributes.drip_has_revenue,
    subscriptions_with_api_enablement.drip_has_api_enabled,
    subscriptions_with_first_payment_value_mrr.drip_first_payment_value_mrr,
    ROUND(store_revenue_stats.revenue_subscription_ytd_usd) AS drip_revenue_subscription_ytd_usd,
    ROUND(store_revenue_stats.revenue_subscription_monthly_average_usd) AS drip_revenue_subscription_monthly_average_usd,
    onsite_campaign_totals.onsite_campaigns_active_current,
    onsite_campaign_totals.onsite_campaigns_total,
    ('https://www.getdrip.com/faucet/users/' || segment_filtered_users_recently_not_churned.drip_user_id) user_faucet_link,
    ('https://www.getdrip.com/faucet/accounts/' || segment_filtered_users_recently_not_churned.drip_account_id) account_faucet_link,
    ('https://www.getdrip.com/faucet/subscriptions/' || segment_filtered_users_recently_not_churned.drip_subscription_id) subscription_faucet_link
FROM "dumps"."dev"."segment_filtered_users_recently_not_churned"
    LEFT JOIN "dumps"."dev"."accounts_with_first_email_sent" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."accounts_with_first_sms_sent" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."accounts_with_legacy_form_activated_events" ON segment_filtered_users_recently_not_churned.drip_account_id = accounts_with_legacy_form_activated_events.account_id
    LEFT JOIN "dumps"."dev"."accounts_with_oms_connected_provider" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."accounts_with_oms_indicated_provider" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."accounts_with_first_person_added" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."accounts_with_api_purchases" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."accounts_with_has_ever_connected_oms" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."accounts_with_installed_snippet_action" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_trial_start_at" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_trial_end_date" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_trust_status" USING (drip_user_id)
    LEFT JOIN "dumps"."dev"."accounts_with_workflow_enabled" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."num_accounts_connected_to_subscription" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_last_payments" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_activation_dates" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_trial_weeks" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."accounts_with_first_registration_date" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."accounts_with_email_campaigns_sent" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."accounts_with_sms_campaigns_sent" USING (drip_account_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_pricing_amounts" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_current_mrr" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_financial_statuses" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_list_size" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_company_information" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_active_workflows" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_sku_count" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_success_managers" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_user_counts" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_sms_subscribers_count" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_industries" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_churn_dates" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_store_revenue_attributes" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_api_enablement" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."subscriptions_with_first_payment_value_mrr" USING (drip_subscription_id)
    LEFT JOIN "dumps"."dev"."store_revenue_stats" ON drip_subscription_id = store_revenue_stats.subscription_id
    LEFT JOIN "dumps"."dev"."onsite_campaign_totals" ON drip_account_id = onsite_campaign_totals.account_id