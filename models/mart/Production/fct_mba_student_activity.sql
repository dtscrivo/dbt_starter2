{{ config(materialized='table') }}
{# 
with all_days as (
SELECT email
--, other_email
, id_deal
, pipeline_stage
, date_diff(current_date(), date_closed, day) as dob
, date_diff(date(circle_login), date(date_closed), day) as circle_logged_in
, COALESCE(date_diff(date(current_date()),date(circle_login),day), max_days) as since_circle_login
, COALESCE(date_diff(date(current_date()),date(hub_login),day), max_days) as since_hub_login
, COALESCE(date_diff(date(current_date()),date(mm_login),day), max_days) as since_mm_login
, COALESCE(date_diff(date(current_date()),date(circle_post),day), max_days) as since_post
, COALESCE(date_diff(date(current_date()),date(circle_comment),day), max_days) as since_comment
, COALESCE(date_diff(date(current_date()),date(meeting),day), max_days) as since_meeting
, COALESCE(date_diff(date(current_date()),date(call_inbound),day), max_days) as since_call_inbound
, COALESCE(date_diff(date(current_date()),date(last_live_training),day), max_days) as since_live_training
, date_diff(date(circle_post), date(date_closed), day) as circle_posted
, date_diff(date(first_office_hours), date(date_closed), day) as scheduled_first_office_hours
, date_diff(date(orientation), date(date_closed), day) as scheduled_orientation
, CAST(posts_count AS INTEGER) AS posts_count
, CAST(comments_count AS INTEGER) AS comments_count
FROM bbg-platform.dbt_production_mba.stg_mba_activity_dates2,
(SELECT 500 AS max_days) 
WHERE pipeline_stage != 'Cancelled Student'
-- and email IN ('ran5js@hotmail.com','burtonrubyann@gmail.com')
)

, activity_data AS (
  SELECT
    *,
    -- Weighted values for each activity
    since_circle_login * 5 AS weighted_circle_login,
    since_hub_login * 3 AS weighted_hub_login,
    since_mm_login * 3 AS weighted_mm_login,
    since_post * 3 AS weighted_post,
    since_comment * 3 AS weighted_comment,
    since_meeting * 2 AS weighted_meeting,
    since_call_inbound * 2 AS weighted_call_inbound,
    since_live_training * 5 AS weighted_live_training
  FROM all_days
),
scoring AS (
  SELECT
    *,
    -- Calculate total weighted inactivity
    weighted_circle_login +
    weighted_hub_login +
    weighted_mm_login +
    weighted_post +
    weighted_comment +
    weighted_meeting +
    weighted_live_training +
    weighted_call_inbound AS total_weighted_inactivity,
    -- Max possible score (sum of weights * max_days)
    (5 * 500) + (3 * 500) + (3 * 500) + (3 * 500) + (3 * 500) + (2 * 500) + (5 * 500) + (2 * 500) AS max_score,
    -- Normalize to a 0-100 scale
    100 - (CAST(weighted_circle_login + weighted_hub_login + weighted_mm_login + weighted_post + 
                weighted_comment + weighted_meeting + weighted_live_training + weighted_call_inbound AS FLOAT64) 
           / ((5 * 500) + (3 * 500) + (3 * 500) + (3 * 500) + (3 * 500) + (2 * 500) + (5 * 500) + (2 * 500))) * 100 AS inactivity_score
  FROM activity_data
),
bonus_logic AS (
  SELECT
    *,
    -- Base bonus points
    CASE 
      WHEN posts_count >= 10 AND since_post <= 10 THEN 5 
      WHEN posts_count >= 10 AND since_post > 10 THEN 2 -- Reduced bonus if inactivity exceeds 10 days
      ELSE 0 
    END +
    CASE 
      WHEN comments_count >= 20 AND since_comment <= 10 THEN 10
      WHEN comments_count >= 20 AND since_comment > 10 THEN 5 -- Reduced bonus if inactivity exceeds 10 days
      ELSE 0 
    END AS bonus_points,
    
    -- Adjust inactivity score with the calculated bonus points
    ROUND(LEAST(
      inactivity_score + 
      CASE 
        WHEN posts_count >= 10 AND since_post <= 10 THEN 10 
        WHEN posts_count >= 10 AND since_post > 10 THEN 5 
        ELSE 0 
      END +
      CASE 
        WHEN comments_count >= 20 AND since_comment <= 10 THEN 5 
        WHEN comments_count >= 20 AND since_comment > 10 THEN 2
        ELSE 0 
      END, 
      100), 0) AS adjusted_inactivity_score
  FROM scoring
)

, exclusions as (
SELECT analytics.fnEmail(property_email) as email
  -- property_flag_exclusions
  , contains_substr(property_flag_exclusions, "No Community Login") as exclude_community_login
  , contains_substr(property_flag_exclusions, "Office Hours Not Scheduled") as exclude_office_hours
  , contains_substr(property_flag_exclusions, "No Community Login Recently") as exclude_recent_community_login
  , contains_substr(property_flag_exclusions, "Orientation Not Scheduled") as exclude_orientation
  , contains_substr(property_flag_exclusions, "No Community Post") as excluded_post
FROM `bbg-platform.hubspot2.contact`
where property_flag_exclusions is not null
)

 , countt as (
SELECT b.email
, pipeline_stage
, id_deal
, dob
, adjusted_inactivity_score as score
, inactivity_score
, bonus_points

, posts_count
, comments_count
  # ADD A DOB LESS THAN FOR SOME OF THESE??
  , CASE WHEN pipeline_stage = "Paused Student" then "Orange"

         WHEN dob >= 4 and circle_logged_in IS NULL AND exclude_community_login IS NULL then "Red"
         WHEN dob >= 4 and since_circle_login >= 14 AND exclude_recent_community_login IS NULL then "Red"
         WHEN dob >= 30 and adjusted_inactivity_score >= 80 then 'Green'
             WHEN dob >= 30 then "Purple"
         WHEN dob >= 14 and scheduled_first_office_hours is null AND exclude_office_hours IS NULL then "Red"
         WHEN dob >= 14 and circle_posted is null AND excluded_post IS NULL then "Red"
         
         WHEN dob >= 14 and scheduled_orientation is null AND exclude_orientation IS NULL then "Red"
         WHEN dob >= 2 and circle_logged_in is null AND exclude_community_login IS NULL then "Yellow" 
         WHEN dob >= 2 and since_circle_login >= 10 AND exclude_recent_community_login IS NULL then "Yellow"       
         WHEN dob >= 7 and scheduled_first_office_hours is null AND exclude_office_hours IS NULL then "Yellow"        
         WHEN dob >= 7 and circle_posted is null AND excluded_post IS NULL then "Yellow"
         WHEN dob >= 7 and scheduled_orientation is null AND exclude_orientation IS NULL then "Yellow"
                 

         ELSE "Green" end as flag
  , CASE WHEN pipeline_stage = "Paused Student" then "On Pause"
       
         
         WHEN dob >= 4 and circle_logged_in IS NULL AND exclude_community_login IS NULL then "No Community Login"
         WHEN dob >= 4 and since_circle_login >= 14 AND exclude_recent_community_login IS NULL then "No Recent Community Login"
           WHEN dob >= 30 and adjusted_inactivity_score >= 80 then "Good"
           WHEN dob >= 30 then "Older than 30 days"
         WHEN dob >= 14 and scheduled_first_office_hours is null AND exclude_office_hours IS NULL then "Office Hours Not Scheduled"
         WHEN dob >= 14 and scheduled_orientation is null AND exclude_orientation IS NULL then "Orientation Not Scheduled"
         WHEN dob >= 14 and circle_posted is null AND excluded_post IS NULL then "No Community Post"
--WHEN dob >= 90 then ">90 Days"
        
         WHEN dob >= 2 and circle_logged_in is null AND exclude_community_login IS NULL then "No Community Login" 
         WHEN dob >= 2 and since_circle_login >= 10 AND exclude_recent_community_login IS NULL then "No Recent Community Login"       
         WHEN dob >= 7 and scheduled_first_office_hours is null AND exclude_office_hours IS NULL then "Office Hours Not Scheduled"        
                  WHEN dob >= 7 and scheduled_orientation is null AND exclude_orientation IS NULL then "Orientation Not Scheduled"
         WHEN dob >= 7 and circle_posted is null AND excluded_post IS NULL then "No Community Post"
                 

         ELSE "Good" end as flag_reason


, circle_logged_in
, since_circle_login
, since_hub_login
, since_mm_login
, since_post
, since_comment
, since_meeting
, since_call_inbound
, since_live_training
, circle_posted
, scheduled_first_office_hours
, scheduled_orientation
, exclude_community_login
, exclude_recent_community_login
, exclude_office_hours
, exclude_orientation
, excluded_post

FROM bonus_logic b
LEFT JOIN exclusions e
  on b.email = e.email

where true
  -- and CASE WHEN dob >= 4 and circle_logged_in IS NULL then "Red-login"
  --        WHEN dob >= 2 and circle_logged_in is null then "Yellow- login"
  --        WHEN dob >= 14 and circle_posted is null then "Red-first post"         
  --        WHEN dob >= 7 and circle_posted is null then "Yellow-first post"
  --        WHEN since_circle_login >= 14 then "Red-since login"
  --        WHEN since_circle_login >= 10 then "Yellow- since login"
  --        ELSE null end = "Red-login"
   --    and pipeline_stage = 'Closed Won'
    --  and email IN ('jindal.sachin@hotmail.com','ksullivan1116@aol.com')
 )

, final as (
select email
  -- , case when flag_reason = "On Pause" then "Purple"
  --        when flag_reason IN ("Orientation Not Scheduled","Office Hours Not Scheduled",">90 Days") and score >= 80 then "Green"
  --        when flag_reason IN ("Orientation Not Scheduled",">90 Days") and score >= 70 then "Green"
  --        when dob >= 90 then "Purple"
  --        when dob > 14 and score < 50 then "Yellow"
  --        when dob > 14 and score < 20 then "Red"
  --        else flag end as flag

  -- , case when flag_reason = "On Pause" then "On Pause"
  --          when flag_reason IN ("Orientation Not Scheduled","Office Hours Not Scheduled",">90 Days") and score >= 80 then "Good"
  --        when flag_reason IN ("Orientation Not Scheduled",">90 Days") and score >= 70 then "Good"
  -- when flag_reason = "Good" and score < 50 and dob > 14 then "Check Activity Score"
  --       when flag_reason = "Good" and score < 20 and dob > 14 then "Check Activity Score"

  --       else flag_reason end as flag_reason
  , score
  , flag
  , flag_reason
--, case when score > 85 and flag = "Red" then 1 else 0 end

from countt
)

 
select email
  , flag as property_student_flag
  , flag_reason as property_student_flag_reason
  , score as property_student_activity_score
from final #}

SELECT *
FROM `dbt_production_mba.fct_mba_student_activity`