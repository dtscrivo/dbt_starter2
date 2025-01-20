{{ config(
    materialized='table',
    schema='hubspot_sales'
) }}

with base as (
SELECT 
  f.*,
  DATE(f.dt) AS date_activity,
  COALESCE(
            CASE WHEN f.type = "MEETING" THEN f.direction ELSE NULL END,
            REGEXP_EXTRACT(f.detail, r'Meeting type\s*([^\n\r]+)')) AS type_activity,
  f.location,
  REGEXP_EXTRACT(f.detail, r'Setter:\s*([^,]+)') AS name_setter_booking,
  REGEXP_EXTRACT(f.detail, r'Booking ID:\s*([^\n\r]+)') AS id_oncehub_booking,
  e.email_prime
FROM `bbg-platform.dbt_production_hubspot_engagements.final_deal_engagement` f
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
  , location as meeting_location
  , name_setter_booking
  , id_oncehub_booking
  , email
FROM base
where TRUE
  and type = "MEETING"
  and contains_substr(type_activity, 'Triage') = false