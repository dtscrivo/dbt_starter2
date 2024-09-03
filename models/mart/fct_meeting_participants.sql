{{ config(materialized='table') }}

SELECT Topic as topic
  , Meeting_ID as meeting_id
  , User_Email as host
  , Creation_Time as date_created
  , Start_Time as time_start
  , End_Time as time_end
  , Name_Original_Name as name
  , Join_Time as time_join
  , Leave_Time as time_leave
  , Duration_Minutes_Duplicate as minutes_attended
  , case when lower(Name_Original_Name) LIKE "%admin%" then "admin"
         when Guest = "No" then "coach"
         else "guest" end as role
FROM `bbg-platform.dbt_tscrivo_stage.meeting_details_export`
WHERE TRUE
  and In_Waiting_Room = "No"
  and (Name_Original_Name not like "%Notetaker%" AND Name_Original_Name not like "%notes%")
  -- and topic = 'MBA: Coaching Hours: 5pm PST'
  -- and start_time = '8/27/2024 16:50'
Qualify row_number() over(partition by Topic, Start_Time, Name_Original_Name order by Duration_Minutes_Duplicate desc) = 1
ORDER BY Meeting_ID, Name_Original_Name