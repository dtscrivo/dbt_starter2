{{ config(materialized='table') }}

with times as (
WITH time_data AS (
  SELECT
    DATETIME(TIMESTAMP(start_time), 'America/Phoenix') AS local_datetime,
    id
  FROM
    `zoom_test.report_meetings`
),
times AS (
  SELECT
    local_datetime,
    id,
    CASE
      WHEN EXTRACT(MINUTE FROM local_datetime) < 30 THEN
        DATETIME_TRUNC(local_datetime, HOUR) + INTERVAL 30 MINUTE
      ELSE
        DATETIME_TRUNC(local_datetime, HOUR) + INTERVAL 1 HOUR
    END AS next_half_hour
  FROM
    time_data
)
SELECT
  id,
  FORMAT_DATETIME('%I:%M%p', next_half_hour) AS formatted_time
FROM
  times
)

SELECT -- *,
     r.id
   , p.name as name_participant
   , d.email as email_participant
   , date(d.date_closed) as date_closed
   , case when d.is_cancelled = 1 then d.date_ticket_resolved else null end as date_cancelled
  , DATETIME(timestamp(p.join_time), 'America/Phoenix') as date_join
  , DATETIME(TIMESTAMP(p.leave_time), 'America/Phoenix') as date_leave
  , p.user_email
  , r.dept
  , r.type
  , r.topic
  , case when lower(r.topic) like "%build%" then "Build"
         when lower(r.topic) like "%design%" then "Design"
         when lower(r.topic) like "%get it done%" then "Get It Dont"
         when lower(r.topic) like "%scale%" then "Scale"
         when lower(r.topic) like "%launch%" then "Launch"
         when lower(r.topic) like "%coaching hours%" then "Coaching Hours"
         else null end as pillar

  -- , REGEXP_REPLACE(
  --   REGEXP_EXTRACT(r.topic, r'(\d{1,2}[:\d]*[ap]m)'),
  --   r':', ''
  -- ) AS meeting_time
  , n.formatted_time as start_time
  , r.duration as duration_meeting
  , DATETIME(TIMESTAMP(r.start_time), 'America/Phoenix') as date_start
  , r.participants_count
  , DATETIME(TIMESTAMP( m.end_time), 'America/Phoenix') as date_end
  , u.email as email_host
  , u.display_name as host
  , case when lower(p.name) like "%notetaker%" or lower(p.name) like "%otter%" then 1 else 0 end as is_ai
  , case when p.user_email != "" then 1 else 0 end as is_employee
  , case when lower(p.name) like "%admin%" then 1 else 0 end as is_admin
FROM `bbg-platform.zoom_test.report_meeting_participants` p
left join   `zoom_test.report_meetings` r
  on p.meeting_id = r.id
left join  `zoom_test.report_meetings` m
  on p.meeting_id = m.id
left join `zoom_test.users` u
  on m.host_id = u.id
left join `dbt_tscrivo.fct_hs_deal` d
  on lower(p.name) = lower(d.name_client)
  and program in ("TAA","MBA")
left join times n
  on r.id = n.id
WHERE p.status = "in_meeting"
  -- and r.topic LIKE "MBA:%"
  and r.id is not null

qualify row_number() over(partition by r.topic, r.start_time, p.name ) = 1
order by r.topic, r.start_time desc, p.name