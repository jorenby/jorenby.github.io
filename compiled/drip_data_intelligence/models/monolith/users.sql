WITH name_parts AS
(
  -- This is janky, but we don't actually store first/last names separately in the the Monolith.
  -- We'll duplicate the same naive splitting logic used in the Monolith.
  -- https://github.com/DripEmail/drip/blob/6f75ecee64d0814225e4e8e3d46c6eb1377b1827/app/models/user.rb#L183-L189
  -- "Joshua" -> "Joshua", ""
  -- "Joshua Jorenby" -> "Joshua", "Jorenby"
  -- "Joshua Paul Jorenby" -> "Joshua", "Paul Jorenby"
  --
  -- NOTE: all indexes are 1-based, not 0-based.
  SELECT id
       , CHARINDEX(' ', users.name)                                        index_of_first_space
       , CASE WHEN index_of_first_space = 0
           THEN LEN(users.name)
           ELSE index_of_first_space - 1
         END                                                               first_name_length
       , first_name_length + 1                                             index_of_last_name
       , SUBSTRING(users.name FROM 1 FOR first_name_length)                first_name
       , SUBSTRING(users.name FROM index_of_last_name FOR LEN(users.name)) last_name
  FROM "dumps"."current"."users"
)
SELECT users.*
     , users.id              user_id
     , name_parts.first_name
     , name_parts.last_name
FROM "dumps"."current"."users"
LEFT JOIN name_parts USING (id)