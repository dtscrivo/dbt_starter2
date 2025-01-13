{{ config(materialized='table') }}

WITH base as (
-- WITH meeting_type AS (
--   SELECT e.*,
--          COALESCE(e.direction,
--                   REGEXP_EXTRACT(e.detail, r'Meeting type\s*([^\n\r]+)')) AS type_activity
--   FROM `bbg-platform.dbt_production_hubspot_engagements.final_deal_engagement` e
--   WHERE e.type = "MEETING"
-- ),
-- meeting_type_with_ranks AS (
--   SELECT 
--     f.*,
--     t.type_activity,
--     ABS(TIMESTAMP_DIFF(f.dt, t.dt, MINUTE)) AS time_diff_minutes,
--     ROW_NUMBER() OVER (PARTITION BY f.id_engagement ORDER BY ABS(TIMESTAMP_DIFF(f.dt, t.dt, MINUTE))) AS rank_closest
--   FROM `bbg-platform.dbt_production_hubspot_engagements.final_deal_engagement` f
--   JOIN meeting_type t
--     ON f.email = t.email
--     AND f.type = "CALL"
--     AND f.direction = "OUTBOUND"
--     AND DATE(f.dt) = DATE(t.dt)
-- ),
-- filtered_meeting_type AS (
--   SELECT *
--   FROM meeting_type_with_ranks
--   WHERE rank_closest = 1
-- )

SELECT 
  f.*,
  DATE(f.dt) AS date_activity,
  COALESCE(
            CASE WHEN f.type = "MEETING" THEN f.direction ELSE NULL END,
            REGEXP_EXTRACT(f.detail, r'Meeting type\s*([^\n\r]+)')) AS type_activity,
  m.property_hs_meeting_location,
  REGEXP_EXTRACT(f.detail, r'Setter:\s*([^,]+)') AS name_setter_booking,
  REGEXP_EXTRACT(f.detail, r'Booking ID:\s*([^\n\r]+)') AS id_oncehub_booking,
  e.email_prime
FROM `bbg-platform.dbt_production_hubspot_engagements.final_deal_engagement` f
LEFT JOIN `bbg-platform.hubspot2.engagement_meeting` m
  ON f.id_engagement = m.engagement_id
LEFT JOIN `bbg-platform.dbt_tscrivo.dim_email` e
  ON f.email = e.email_all
-- LEFT JOIN filtered_meeting_type t
--   ON f.id_engagement = t.id_engagement
)


SELECT id_engagement
  , dt as date_meeting
  , id_owner
  , disposition
  , date_created
  , status
  , source
  , name_owner
  , team
  , id_contact
  , type_activity
  , property_hs_meeting_location as meeting_location
  , name_setter_booking
  , id_oncehub_booking
  , email
FROM base
where TRUE
  and type = "MEETING"
  and contains_substr(type_activity, 'Triage') = false