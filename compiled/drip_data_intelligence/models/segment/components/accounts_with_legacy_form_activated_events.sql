WITH legacy_form_activated_events AS (
    SELECT account_id
         , action
    FROM "dumps"."dev"."account_events"
    WHERE 1 = 1
      AND action IN (
                     'Embedded form activated',
                     'Exit intent form activated',
                     'Popup form activated',
                     'SMS form activated',
                     'Side tab form activated'
        )
    )
SELECT DISTINCT
     a.id                      account_id
   , (lfae.action IS NOT NULL) has_legacy_form_activated_event
FROM "dumps"."dev"."accounts" a
     LEFT JOIN legacy_form_activated_events lfae ON a.id = lfae.account_id