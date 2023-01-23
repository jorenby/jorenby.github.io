WITH RECURSIVE pre_calendar(n) AS (
      SELECT 1 :: integer
       UNION
           ALL
      SELECT n + 1
        FROM pre_calendar
       WHERE n < datediff(DAYS, '2010-jan-1', 
    

    
        CURRENT_DATE
    
)
  )
     , calendar AS (
      SELECT trunc(dateadd('days', -n, 
    

    
        CURRENT_DATE
    
)) calendar_date
        FROM pre_calendar
  )
     , calendar2 AS (
      SELECT c1.calendar_date as_of_date
           , c2.calendar_date events_date
           , CASE
                 WHEN c1.calendar_date = c2.calendar_date
                     THEN 1
                     ELSE 0
          END                 day_of_ind
           , CASE
                 WHEN c1.calendar_date - 7 < c2.calendar_date
                     THEN 1
                     ELSE 0
          END                 rolling_7
           , CASE
                 WHEN c1.calendar_date - 30 < c2.calendar_date
                     THEN 1
                     ELSE 0
          END                 rolling_30
        FROM calendar c1
                 JOIN calendar c2 ON c1.calendar_date >= c2.calendar_date
  )
SELECT *
  FROM calendar2