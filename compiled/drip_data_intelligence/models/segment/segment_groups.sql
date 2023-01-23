-- WARNING: Column names and values are directly reflected in `group` calls made to Segment
--   Don't change anything unless you're super sure you know what you're doing
  WITH account_public_ids AS (
     SELECT DISTINCT account_public_id
     FROM "dumps"."dev"."segment_user_group_mapping" sgum
  )
  , amc AS (
   SELECT DISTINCT -- TODO: this is an evil kludge. AMC has duplicates and shouldn't. We need to move that to dbt and fix it
      account_id
      , total_billable_subscribers
      , estimated_mrr
   FROM "dumps"."dev"."account_metrics_current"
  )
  , sba AS (
   SELECT DISTINCT -- TODO: Kludge. SBA really should be 1:1 with subscriptions. https://getdrip.slack.com/archives/C011JR7BA6A/p1668731978649909
      subscription_id
      , agency
   FROM current.subscription_billing_attributes
  )
  , feature_flags AS (
   SELECT
      account_id
      , listagg(flag_name, ',') AS flags
   FROM current.feature_flags
   GROUP BY account_id
  )
SELECT a.public_id                                                      account_public_id
     , a.public_id                                                      id
     , amc.total_billable_subscribers                                   account_total_email_subscribers
     , a.created_at
     , a.default_postal_address                                         address
     , amc.estimated_mrr                                                estimated_account_mrr
     , a.name
     , u.email
     , s.plan_id                                                        plan
     , s.public_id                                                      subscription_public_id
     , swcm.drip_current_mrr * 100                                      subscription_mrr
     , subscription_owner_user.public_id                                subscription_owner_public_id
     , swls.drip_email_list_size                                        subscription_total_email_subscribers
     , a.url                                                            website
     , COALESCE(u.public_id, subscription_owner_user.public_id)         user_public_id
     , sba.agency                                                       agency_managed -- TODO: get this from PartnerStack when we can
     , onsite_campaign_totals.onsite_campaigns_active_current
     , onsite_campaign_totals.onsite_campaigns_total
     , revenue_account_monthly_average_usd
     , revenue_subscription_monthly_average_usd
     , revenue_account_ytd_usd
     , revenue_subscription_ytd_usd
     , a.status                                                         account_monolith_status
     , s.status                                                         subscription_monolith_status
     , mss.subscription_status                                          subscription_marketing_status
     , ('https://www.getdrip.com/faucet/accounts/' || a.public_id)      account_faucet_link
     , ('https://www.getdrip.com/faucet/subscriptions/' || s.public_id) subscription_faucet_link
     , feature_flags.flags                                              feature_flags
     , (oms.drip_oms_connected_provider IS NOT NULL)                    oms_connected
     , oms.drip_oms_connected_provider                                  oms_connected_provider
     , legacy_form_events.has_legacy_form_activated_event
     , legacy_form_totals.legacy_forms_active_current
     , legacy_form_totals.legacy_forms_total
  FROM account_public_ids
     LEFT JOIN "dumps"."dev"."accounts" a ON account_public_ids.account_public_id = a.public_id
     LEFT JOIN "dumps"."dev"."subscriptions" s ON a.subscription_id = s.id
     LEFT JOIN "dumps"."dev"."subscriptions_with_current_mrr" swcm ON s.id = swcm.drip_subscription_id
     LEFT JOIN amc ON a.id = amc.account_id
     LEFT JOIN "dumps"."dev"."memberships" m ON a.id = m.account_id AND m.role = 'owner'
     LEFT JOIN "dumps"."dev"."users" u ON m.user_id = u.id
     LEFT JOIN "dumps"."dev"."users" subscription_owner_user ON s.user_id = subscription_owner_user.id
     LEFT JOIN sba ON s.id = sba.subscription_id
     LEFT JOIN "dumps"."dev"."onsite_campaign_totals" ON a.id = onsite_campaign_totals.account_id
     LEFT JOIN "dumps"."dev"."store_revenue_stats" srs ON a.id = srs.account_id
     LEFT JOIN "dumps"."dev"."marketing_subscription_status" mss ON s.id = mss.drip_subscription_id
     LEFT JOIN "dumps"."dev"."subscriptions_with_list_size" swls ON s.id = swls.drip_subscription_id
     LEFT JOIN feature_flags ON a.id = feature_flags.account_id
     LEFT JOIN "dumps"."dev"."accounts_with_oms_connected_provider" oms ON a.id = oms.drip_account_id
     LEFT JOIN "dumps"."dev"."accounts_with_legacy_form_activated_events" legacy_form_events ON a.id = legacy_form_events.account_id
     LEFT JOIN "dumps"."dev"."legacy_form_totals" ON a.id = legacy_form_totals.account_id