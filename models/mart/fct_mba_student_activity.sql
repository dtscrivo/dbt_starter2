{{ config(materialized='table') }}

WITH all_days AS (
WITH base as (
WITH activity as (
select type
, max(timestamp) as dt

, email_prime as email
, direction
-- , disposition 
-- , subject
from `dbt_tscrivo.fct_hs_engagements_agg` h
LEFT JOIN `dbt_tscrivo.dim_email` e
  on h.email = e.email_all
-- where email = 'luckhappens@msn.com'

GROUP BY ALL
order by email

)

, office_hours as (
select timestamp as first_office_hours
, email_prime as email
-- , disposition
, subject

from `dbt_tscrivo.fct_hs_engagements_agg` h
LEFT JOIN `dbt_tscrivo.dim_email` e
  on h.email = e.email_all
LEFT JOIN `dbt_tscrivo.fct_hs_deal` d
  on e.email_all = d.email
where TRUE
  and type = "MEETING"
 -- and h.email = 'luckhappens@msn.com'
  and subject like "%Office Hours%"
  and d.date_closed < h.timestamp
qualify row_number() over(partition by h.email order by timestamp asc) = 1
order by email
)

, live_training AS (
WITH recent_participation AS (
  SELECT 
    email_participant AS email,
    topic,
    pillar,
    date_start,
    name_participant,
    ROW_NUMBER() OVER (PARTITION BY email_participant ORDER BY date_start DESC) AS row_num
  FROM `bbg-platform.dbt_production.fct_zoom_meeting_participation_airbyte`
  WHERE topic LIKE "MBA:%"
    AND email_participant IS NOT NULL
)
SELECT 
  e.email_prime as email,
  topic,
  pillar,
  date_start as last_live_training
FROM recent_participation p
LEFT JOIN `dbt_tscrivo.dim_email` e
  on p.email = e.email_all
WHERE row_num = 1
)

, orientation as (
select timestamp as orientation
, email_prime as email
-- , disposition
, subject

from `dbt_tscrivo.fct_hs_engagements_agg` h
LEFT JOIN `dbt_tscrivo.dim_email` e
  on h.email = e.email_all
LEFT JOIN `dbt_tscrivo.fct_hs_deal` d
  on e.email_all = d.email
where TRUE
  and type = "MEETING"
 -- and h.email = 'luckhappens@msn.com'
  and subject like "%Mastermind Business Academy Orientation%"
  and d.date_closed < h.timestamp
qualify row_number() over(partition by h.email order by timestamp asc) = 1
order by email
)


, contacts as (
  # Contact Table Cleanup
WITH ids_to_exclude AS (
  SELECT DISTINCT CAST(id AS INT64) AS id
  FROM `bbg-platform.hubspot2.contact`,
  UNNEST(SPLIT(REPLACE(property_hs_merged_object_ids, ';', ','), ',')) AS id
  WHERE SAFE_CAST(id AS INT64) IS NOT NULL
)
# removes merged and deleted contacts
SELECT *
  , analytics.fnEmail(c.property_email) as email
FROM `bbg-platform.hubspot2.contact` c
WHERE true
  AND is_deleted = FALSE
  and property_email is not null and property_email != ""
  AND id NOT IN (
    SELECT id
    FROM ids_to_exclude
  )
)


, members as (
  select max(last_seen_at) as last_login
  , e.email_prime as email
  , posts_count
  , comments_count
  from `bbg-platform.circle_api.members` m
  LEFT JOIN `dbt_tscrivo.dim_email` e
  on analytics.fnEmail(m.email) = e.email_all
 -- where email = 'burtonrubyann@gmail.com'
  GROUP BY ALL
)

, posts as (
SELECT e.email_prime as email
, MAX(DATETIME(TIMESTAMP(p.published_at), "America/Phoenix")) AS latest_post
FROM `bbg-platform.circle_api.posts` p
LEFT JOIN `bbg-platform.circle_api.members` m
on p.user_id = m.user_id
LEFT JOIN `dbt_tscrivo.dim_email` e
  on analytics.fnEmail(m.email) = e.email_all
WHERE m.email IS NOT NULL
--and m.email = 'burtonrubyann@gmail.com'
GROUP BY ALL
)

, comments as (
SELECT e.email_prime as email
, max(DATETIME(TIMESTAMP(p.created_at), "America/Phoenix")) as latest_comment
FROM `bbg-platform.circle_api.comments` p
LEFT JOIN `bbg-platform.circle_api.members` m
on p.user_id = m.user_id
LEFT JOIN `dbt_tscrivo.dim_email` e
  on analytics.fnEmail(m.email) = e.email_all
WHERE m.email IS NOT NULL
-- and m.email = 'burtonrubyann@gmail.com'
GROUP BY ALL
)



, lasts as (
SELECT e.email_prime as email
, max(property_last_gg_convo) as gg_convo
, max(property_last_hub_login) as hub_login
, max(property_last_mastermind_login) as mm_login
, max(property_last_mbs_watch_date) as mbs_watch
, max(property_circle_last_post_date) as circle_post
--, max(property_circle_last_login_date) as circle_login
, m.last_login as circle_login
, property_circle_sync
, max(property_circle_last_event_rsvp_date) as circle_rsvp
, m.posts_count
, m.comments_count
FROM contacts c
LEFT JOIN `dbt_tscrivo.dim_email` e
  on c.email = e.email_all
LEFT JOIN members m 
  on e.email_all = m.email 
--where e.email_prime = 'burtonrubyann@gmail.com'
GROUP BY all
--qualify row_number() over(partition by email order by case when property_last_mastermind_login is null then 0 else 1 end desc) = 1
)

SELECT e.email_prime as email
, d.email as other_email
, pipeline_stage
, d.date_closed
, gg_convo
, hub_login
, mm_login
, mbs_watch
, coalesce(datetime(circle_post), latest_post) as circle_post
, circle_login
, property_circle_sync
, circle_rsvp
, am.dt as meeting
, ac.dt as call_inbound
, c.latest_comment as circle_comment
, oh.first_office_hours
, o.orientation
, l.comments_count
, l.posts_count
, lt.last_live_training
, d.id_deal
FROM `dbt_tscrivo.fct_hs_deal` d
LEFT JOIN `dbt_tscrivo.dim_email` e
  on case when d.id_deal = 45235040447 then 'jonstankorb@gmail.com' else d.email end = e.email_all
LEFT JOIN activity am
on e.email_prime = am.email
and am.type = 'MEETING'
LEFT JOIN activity ac
on e.email_prime = ac.email
and ac.type = 'CALL'
and ac.direction = 'inbound'
LEFT JOIN lasts l
on e.email_prime = l.email
LEFT JOIN posts p
on e.email_prime = p.email
LEFT JOIN comments c
on e.email_prime = c.email
LEFT JOIN office_hours oh
  on e.email_prime = oh.email
LEFT JOIN orientation o
  on e.email_prime = o.email
LEFT JOIN live_training lt
  on e.email_prime = lt.email
WHERE d.program IN ('MBA','TAA')
and d.is_buyer = 1
and d.is_inperson = 0
-- and d.email = 'oonawest@hotmail.com'
GROUP BY all
)

SELECT email
--, other_email
, id_deal
, pipeline_stage
, date_diff(current_date(), date_closed, day) as dob
, date_diff(date(base.circle_login), date(date_closed), day) as circle_logged_in
, COALESCE(date_diff(date(current_date()),date(circle_login),day), max_days) as since_circle_login
, COALESCE(date_diff(date(current_date()),date(hub_login),day), max_days) as since_hub_login
, COALESCE(date_diff(date(current_date()),date(mm_login),day), max_days) as since_mm_login
, COALESCE(date_diff(date(current_date()),date(circle_post),day), max_days) as since_post
, COALESCE(date_diff(date(current_date()),date(circle_comment),day), max_days) as since_comment
, COALESCE(date_diff(date(current_date()),date(meeting),day), max_days) as since_meeting
, COALESCE(date_diff(date(current_date()),date(call_inbound),day), max_days) as since_call_inbound
, COALESCE(date_diff(date(current_date()),date(last_live_training),day), max_days) as since_live_training
, date_diff(date(base.circle_post), date(date_closed), day) as circle_posted
, date_diff(date(first_office_hours), date(date_closed), day) as scheduled_first_office_hours
, date_diff(date(orientation), date(date_closed), day) as scheduled_orientation
, CAST(posts_count AS INTEGER) AS posts_count
, CAST(comments_count AS INTEGER) AS comments_count
FROM base,
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


 , countt as (
SELECT email
, pipeline_stage
, id_deal
, dob
, adjusted_inactivity_score as score
, inactivity_score
, bonus_points

, posts_count
, comments_count
  # ADD A DOB LESS THAN FOR SOME OF THESE??
  , CASE WHEN pipeline_stage = "Paused Student" then "Purple"
       --  WHEN dob >= 90 then "Purple"
         WHEN dob >= 4 and circle_logged_in IS NULL then "Red"
         WHEN dob >= 4 and since_circle_login >= 14 then "Red"
         WHEN dob >= 14 and scheduled_first_office_hours is null then "Red"
         WHEN dob >= 14 and circle_posted is null then "Red"
         
         WHEN dob >= 14 and scheduled_orientation is null then "Red"
         WHEN dob >= 2 and circle_logged_in is null then "Yellow" 
         WHEN dob >= 2 and since_circle_login >= 10 then "Yellow"       
         WHEN dob >= 7 and scheduled_first_office_hours is null then "Yellow"        
         WHEN dob >= 7 and circle_posted is null then "Yellow"
         WHEN dob >= 7 and scheduled_orientation is null then "Yellow"
                 

         ELSE "Green" end as flag
  , CASE WHEN pipeline_stage = "Paused Student" then "On Pause"
         
         
         WHEN dob >= 4 and circle_logged_in IS NULL then "No Community Login"
         WHEN dob >= 4 and since_circle_login >= 14 then "No Recent Community Login"
         WHEN dob >= 14 and scheduled_first_office_hours is null then "Office Hours Not Scheduled"
         WHEN dob >= 14 and scheduled_orientation is null then "Orientation Not Scheduled"
         WHEN dob >= 14 and circle_posted is null then "No Community Post"
         WHEN dob >= 90 then ">90 Days"
        
         WHEN dob >= 2 and circle_logged_in is null then "No Community Login" 
         WHEN dob >= 2 and since_circle_login >= 10 then "No Recent Community Login"       
         WHEN dob >= 7 and scheduled_first_office_hours is null then "Office Hours Not Scheduled"        
                  WHEN dob >= 7 and scheduled_orientation is null then "Orientation Not Scheduled"
         WHEN dob >= 7 and circle_posted is null then "No Community Post"
                 

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


FROM bonus_logic

where true
  -- and CASE WHEN dob >= 4 and circle_logged_in IS NULL then "Red-login"
  --        WHEN dob >= 2 and circle_logged_in is null then "Yellow- login"
  --        WHEN dob >= 14 and circle_posted is null then "Red-first post"         
  --        WHEN dob >= 7 and circle_posted is null then "Yellow-first post"
  --        WHEN since_circle_login >= 14 then "Red-since login"
  --        WHEN since_circle_login >= 10 then "Yellow- since login"
  --        ELSE null end = "Red-login"
   --    and pipeline_stage = 'Closed Won'
       -- and email = 'chris@thephilosophylab.com'
 )

, final as (
select email
  , case when flag_reason = "On Pause" then "Purple"
         when flag_reason IN ("Orientation Not Scheduled","Office Hours Not Scheduled") and score >= 80 then "Green"
         when flag_reason IN ("Orientation Not Scheduled") and score >= 70 then "Green"
         when dob >= 90 then "Purple"
         else flag end as flag
  , flag_reason
  , score
  , case when score > 85 and flag = "Red" then 1 else 0 end
--from `dbt_tscrivo.fct_mba_student_activity`
from countt
)

select email
  , flag as property_student_flag
  , flag_reason as property_student_flag_reason
  , score as property_student_activity_score
from final