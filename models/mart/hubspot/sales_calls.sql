{{ config(
    materialized='table',
    schema='hubspot_sales'
) }}

WITH base as (
WITH meeting_type AS (
  SELECT e.*,
         COALESCE(e.direction,
                  REGEXP_EXTRACT(e.detail, r'Meeting type\s*([^\n\r]+)')) AS type_activity
  FROM `bbg-platform.dbt_production_hubspot_engagements.final_deal_engagement` e
  WHERE e.type = "MEETING"
),
meeting_type_with_ranks AS (
  SELECT 
    f.*,
    t.type_activity,
    ABS(TIMESTAMP_DIFF(f.dt, t.dt, MINUTE)) AS time_diff_minutes,
    ROW_NUMBER() OVER (PARTITION BY f.id_engagement ORDER BY ABS(TIMESTAMP_DIFF(f.dt, t.dt, MINUTE))) AS rank_closest
  FROM `bbg-platform.dbt_production_hubspot_engagements.final_deal_engagement` f
  JOIN meeting_type t
    ON f.email = t.email
    AND f.type = "CALL"
    AND f.direction = "OUTBOUND"
    AND DATE(f.dt) = DATE(t.dt)
),
filtered_meeting_type AS (
  SELECT *
  FROM meeting_type_with_ranks
  WHERE rank_closest = 1
)
SELECT 
  f.*,
  DATE(f.dt) AS date_activity,
  COALESCE(COALESCE(
            CASE WHEN f.type = "MEETING" THEN f.direction ELSE NULL END,
            REGEXP_EXTRACT(f.detail, r'Meeting type\s*([^\n\r]+)')), t.type_activity) AS type_activity,
  CASE WHEN f.type = 'CALL' AND f.disposition IN ('connected', 'contact hung up', 'discovery call booked', 'discovery call offered', 'disqualified', 'follow up booked', 'follow up offered', 'follow up task') THEN 1 ELSE 0 END AS is_connected
  , CASE WHEN f.type = 'CALL' and f.disposition = 'discovery call booked' then 1 else 0 end as is_qualified
  , REGEXP_EXTRACT(f.detail, r'Setter:\s*([^,]+)') AS name_setter_booking,
  REGEXP_EXTRACT(f.detail, r'Booking ID:\s*([^\n\r]+)') AS id_oncehub_booking,
  e.email_prime
  , dense_rank() over(partition by f.email, f.type, date(f.dt) order by f.dt desc) as call_num 
FROM `bbg-platform.dbt_production_hubspot_engagements.final_deal_engagement` f
LEFT JOIN `bbg-platform.dbt_tscrivo.dim_email` e
  ON f.email = e.email_all
LEFT JOIN filtered_meeting_type t
  ON f.id_engagement = t.id_engagement
)

SELECT id_engagement
  , dt
  , id_owner
  , disposition
  , id_team
  , status
  , source
  , name_owner
  , id_contact
  , type_activity
  , is_connected
  , email
  , call_num
  , is_qualified
  , case when call_num = 1 AND contains_substr(type_activity, 'Triage') then 1 else 0 end as is_triage
FROM base
WHERE type = 'CALL'
  and direction = 'OUTBOUND'
qualify row_number() over(partition by dt, email) = 1