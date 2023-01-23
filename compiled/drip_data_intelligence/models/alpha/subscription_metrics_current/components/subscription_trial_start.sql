SELECT id                                                  AS subscription_id,
       created_at                                          AS trial_lead_acquired_at,
       DATE_ADD('day', -1, DATE_TRUNC('week', created_at)) AS trial_lead_week_cohort,
       started_at                                          AS corrected_trial_start_at,
       DATE_ADD('day', -1, DATE_TRUNC('week', started_at)) AS corrected_trial_week_cohort
FROM "dumps"."dev"."subscriptions"