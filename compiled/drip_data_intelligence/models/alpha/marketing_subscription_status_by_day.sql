WITH state_transitions_combined AS
      (
          SELECT subject_id
               , "from"
               , "to"
               , created_at
            FROM data_intelligence.subscription_state_transitions
           WHERE TRUE
             AND "to" NOT LIKE 'trust%'
             AND "from" NOT LIKE 'trust%'
           UNION ALL
          SELECT id           AS subject_id
               , NULL         AS "from"
               , 'setting_up' AS "to"
               , created_at
            FROM "dumps"."dev"."subscriptions"
           WHERE TRUE
             AND status = 'setting_up'
      )
     , backfill_state_transitions AS
      (
          SELECT subject_id
               , "from"
               , "to"
               , created_at
               , as_of_date
               , events_date
               , row_number() OVER (PARTITION BY subject_id, as_of_date ORDER BY events_date DESC) n
               , lag(created_at) OVER (PARTITION BY subject_id, as_of_date ORDER BY events_date)   penultimate_created_at
            FROM state_transitions_combined
                     JOIN "dumps"."dev"."backfill_calendar" ON events_date = date_trunc('d', created_at)
           WHERE as_of_date >= '1-jan-2020'
      )
     , state_transitions_by_date AS
      (
          SELECT subject_id
               , "from"                                             last_status_from
               , "to"                                               last_status_to
               , created_at
               , as_of_date
               , date_diff('d', penultimate_created_at, created_at) days_between_state_change
            FROM backfill_state_transitions
           WHERE n = 1
      )
     , trust_status_backfill AS
      (
          SELECT subject_id
               , "to"
               , "from"
               , created_at
               , events_date
               , as_of_date
               , row_number() OVER (PARTITION BY subject_id, as_of_date ORDER BY events_date DESC) n
               , lag(created_at) OVER (PARTITION BY subject_id, as_of_date ORDER BY events_date)   penultimate_created_at
            FROM data_intelligence.subscription_state_transitions
                     JOIN "dumps"."dev"."backfill_calendar" ON date_trunc('d', created_at) = events_date
           WHERE TRUE
             AND "to" LIKE 'trust%'
             AND "from" LIKE 'trust%'
             AND as_of_date >= '1-jan-2020'
      )
     , trust_status_by_date AS
      (
          SELECT subject_id
               , created_at
               , "from" AS                                          last_trust_status_from
               , "to"   AS                                          last_trust_status_to
               , as_of_date
               , date_diff('d', penultimate_created_at, created_at) days_between_trust_change
            FROM trust_status_backfill
           WHERE TRUE
             AND n = 1
      )
SELECT calendar_date                                                               AS  as_of_date
     , coalesce(state_transitions_by_date.subject_id, trust_status_by_date.subject_id) subscription_id
     , last_status_from
     , last_status_to
     , last_trust_status_from
     , last_trust_status_to
     , days_between_state_change
     , days_between_trust_change
     , CASE
           WHEN (last_trust_status_to = 'trust.banned'
               OR last_trust_status_to = 'trust.blocked')
               AND days_between_trust_change NOT IN (89, 90, 91)
               THEN -- Check for 90 day block/ban
               'banned'
               ELSE
               NULL
    END                                                                            AS  is_banned_or_blocked -- Anyone legitimately banned or blocked should have a status of 'banned' (COALESCE BELOW)
     , CASE
           WHEN last_status_from = 'trial_expired'
               AND last_status_to = 'cancelled'
               AND days_between_state_change IN (13, 14, 15)
               THEN -- check for 14 day auto cancellation
               'trial_expired'
               ELSE
               NULL
    END                                                                            AS  had_trial_expire     -- If they aren't banned and the last legitimate status was trial_expired, they are trial expired (COALESCE BELOW)
     , CASE WHEN last_status_to = 'cancelled' THEN 'churned' ELSE last_status_to END AS  cancelled_to_churned
     , coalesce(is_banned_or_blocked, had_trial_expire, cancelled_to_churned)      AS  final_status
  FROM "dumps"."alpha"."dim_date"
           LEFT JOIN state_transitions_by_date ON state_transitions_by_date.as_of_date = calendar_date
           LEFT JOIN trust_status_by_date ON trust_status_by_date.as_of_date = calendar_date
      AND state_transitions_by_date.subject_id = trust_status_by_date.subject_id