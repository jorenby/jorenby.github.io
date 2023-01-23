SELECT subscription_id,
       activated AND (trial_expired_at IS NULL OR activated_at <= trial_expired_at) AS activated_within_trial,
       activated AND activated_at > trial_expired_at                               AS activated_outside_trial
FROM "dumps"."dev"."subscriptions" s
         JOIN "dumps"."current"."subscription_details" sd ON s.id = sd.subscription_id
         LEFT JOIN (SELECT subject_id AS subscription_id, MIN(created_at) AS trial_expired_at
                    FROM "dumps"."data_intelligence"."subscription_state_transitions"
                    WHERE "to" = 'trial_expired'
                    GROUP BY subject_id) USING (subscription_id)