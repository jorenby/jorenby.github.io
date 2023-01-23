WITH base AS (SELECT * FROM "dumps"."dev"."subscription_id"),
     activated_within_trial AS (SELECT * FROM "dumps"."dev"."subscription_activated_within_trial"),
     churned_before_n_days_active AS (SELECT * FROM "dumps"."dev"."subscription_churned_before_n_days_active"),
     created_by_drip_employee AS (SELECT * FROM "dumps"."dev"."subscription_created_by_drip_employee"),
     details AS (SELECT * FROM "dumps"."dev"."subscription_details"),
     dimensions AS (SELECT * FROM "dumps"."dev"."subscription_dimensions"),
     first_churned_at AS (SELECT * FROM "dumps"."dev"."subscription_first_churned_at"),
     has_marketing_attribution AS (SELECT * FROM "dumps"."dev"."subscription_has_marketing_attribution"),
     indicated_an_oms AS (SELECT * FROM "dumps"."dev"."subscription_indicated_an_oms"),
     indicated_an_oms_non_paid_marketing AS (SELECT * FROM "dumps"."dev"."subscription_indicated_an_oms_non_paid_marketing"),
     indicated_an_oms_paid_marketing AS (SELECT * FROM "dumps"."dev"."subscription_indicated_an_oms_paid_marketing"),
     is_unblocked AS (SELECT * FROM "dumps"."dev"."subscription_is_unblocked"),
     oms_indicated AS (SELECT * FROM "dumps"."dev"."subscription_oms_indicated"),
     oms_indicated_backfilled AS (SELECT * FROM "dumps"."dev"."subscription_oms_indicated_backfilled"),
     pct_trial_lead_attributed_to_direct_marketing AS (SELECT * FROM "dumps"."dev"."subscription_pct_trial_lead_attributed_to_direct_marketing"),
     pct_trial_lead_attributed_to_organic_marketing AS (SELECT * FROM "dumps"."dev"."subscription_pct_trial_lead_attributed_to_organic_marketing"),
     pct_trial_lead_attributed_to_other_marketing AS (SELECT * FROM "dumps"."dev"."subscription_pct_trial_lead_attributed_to_other_marketing"),
     pct_trial_lead_attributed_to_paid_marketing AS (SELECT * FROM "dumps"."dev"."subscription_pct_trial_lead_attributed_to_paid_marketing"),
     trial_ended_at AS (SELECT * FROM "dumps"."dev"."subscription_trial_ended_at"),
     trial_start AS (SELECT * FROM "dumps"."dev"."subscription_trial_start"),
     trust_status AS (SELECT * FROM "dumps"."dev"."subscription_trust_status")
SELECT * FROM base
    LEFT JOIN activated_within_trial USING (subscription_id)
    LEFT JOIN churned_before_n_days_active USING (subscription_id)
    LEFT JOIN created_by_drip_employee USING (subscription_id)
    LEFT JOIN details USING (subscription_id)
    LEFT JOIN dimensions USING (subscription_id)
    LEFT JOIN first_churned_at USING (subscription_id)
    LEFT JOIN has_marketing_attribution USING (subscription_id)
    LEFT JOIN indicated_an_oms USING (subscription_id)
    LEFT JOIN indicated_an_oms_non_paid_marketing USING (subscription_id)
    LEFT JOIN indicated_an_oms_paid_marketing USING (subscription_id)
    LEFT JOIN is_unblocked USING (subscription_id)
    LEFT JOIN oms_indicated USING (subscription_id)
    LEFT JOIN oms_indicated_backfilled USING (subscription_id)
    LEFT JOIN pct_trial_lead_attributed_to_direct_marketing USING (subscription_id)
    LEFT JOIN pct_trial_lead_attributed_to_organic_marketing USING (subscription_id)
    LEFT JOIN pct_trial_lead_attributed_to_other_marketing USING (subscription_id)
    LEFT JOIN pct_trial_lead_attributed_to_paid_marketing USING (subscription_id)
    LEFT JOIN trial_ended_at USING (subscription_id)
    LEFT JOIN trial_start USING (subscription_id)
    LEFT JOIN trust_status USING (subscription_id)