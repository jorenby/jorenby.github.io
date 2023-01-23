WITH recursive tmp(account_public_id, updated_at, DataItem, builtwith_response_json) AS
                   (
                       SELECT account_public_id,
                              updated_at,
                              substring(builtwith_response_json,
                                        CHARINDEX('''Name'':', builtwith_response_json) + 9,
                                        charindex('''Description'':', builtwith_response_json) -
                                        CHARINDEX('''Name'':', builtwith_response_json)),
                              right(builtwith_response_json,
                                    len(builtwith_response_json) -
                                    charindex('''Description'':', builtwith_response_json) - 10)
                       FROM current.account_technology_usages
                       where charindex('''Description'':', builtwith_response_json) >
                             CHARINDEX('''Name'':', builtwith_response_json)
                         and updated_at::DATE > '2020-1-1'
                       UNION all
                       SELECT account_public_id,
                              updated_at,
                              substring(builtwith_response_json,
                                        CHARINDEX('''Name'':', builtwith_response_json) + 9,
                                        charindex('''Description'':', builtwith_response_json) -
                                        CHARINDEX('''Name'':', builtwith_response_json)),
                              right(builtwith_response_json,
                                    len(builtwith_response_json) -
                                    charindex('''Description'':', builtwith_response_json) - 10)
                       FROM tmp
                       WHERE CHARINDEX('''Name'':', builtwith_response_json) > 0
                         and charindex('''Description'':', builtwith_response_json) > 0
                         and charindex('''Description'':', builtwith_response_json) >
                             CHARINDEX('''Name'':', builtwith_response_json)
                   ),
     tmp2(account_public_id, updated_at, DataItem, builtwith_response_json) AS
                   (
                       SELECT account_public_id,
                              updated_at,
                              substring(builtwith_response_json,
                                        CHARINDEX('"Name":', builtwith_response_json) + 9,
                                        charindex('"Description":', builtwith_response_json) -
                                        CHARINDEX('"Name":', builtwith_response_json)),
                              right(builtwith_response_json,
                                    len(builtwith_response_json) -
                                    charindex('"Description":', builtwith_response_json) - 10)
                       FROM current.account_technology_usages
                       where charindex('"Description":', builtwith_response_json) >
                             CHARINDEX('"Name":', builtwith_response_json)
                         and updated_at::DATE > '2020-1-1'
                       UNION all
                       SELECT account_public_id,
                              updated_at,
                              substring(builtwith_response_json,
                                        CHARINDEX('"Name":', builtwith_response_json) + 9,
                                        charindex('"Description":', builtwith_response_json) -
                                        CHARINDEX('"Name":', builtwith_response_json)),
                              right(builtwith_response_json,
                                    len(builtwith_response_json) -
                                    charindex('"Description":', builtwith_response_json) - 10)
                       FROM tmp2
                       WHERE CHARINDEX('"Name":', builtwith_response_json) > 0
                         and charindex('"Description":', builtwith_response_json) > 0
                         and charindex('"Description":', builtwith_response_json) >
                             CHARINDEX('"Name":', builtwith_response_json)
                   )
SELECT account_public_id,
       updated_at,
       left(DataItem, case when len(dataitem) > 13 then len(DataItem) - 12 else 1 end) technology_detected
FROM tmp
union all
select account_public_id,
       updated_at,
       left(DataItem, case when len(dataitem) > 13 then len(DataItem) - 12 else 1 end) technology_detected
from tmp2