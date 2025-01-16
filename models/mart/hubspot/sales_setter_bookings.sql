{{ config(
    materialized='table',
    schema='hubspot_sales'
) }}

SELECT f.*
  , DATE(f.dt) as date_activity
  , COALESCE(direction,
    REGEXP_EXTRACT(detail, r'Meeting type\s*([^\n\r]+)')) as type_meeting_activity
  , f.direction as type_activity
  , l.date_setter_lead_assigned
  , TRIM(REGEXP_EXTRACT(detail, r'Setter:\s*([^,]+)')) AS name_setter_booking
  , TRIM(REGEXP_EXTRACT(detail, r'Booking ID:\s*([^\n\r]+)')) AS id_oncehub_booking
  , b.status_meeting
  , f.date_created as date_booking
FROM `dbt_production_hubspot_engagements.final_deal_engagement` f
LEFT JOIN `bbg-platform.dbt_production_hubspot_lead_deals.final_leads_present_status` l
  on f.id_contact = l.id_contact
LEFT JOIN `bbg-platform`.`dbt_production_hubspot_lead_deals`.`stg_oncehub_bookings` b
  on REGEXP_EXTRACT(detail, r'Booking ID:\s*([^\n\r]+)') = b.final_id_booking
where TRUE
  and f.type = "MEETING"
  and REGEXP_EXTRACT(detail, r'Setter:\s*([^,]+)') IS NOT NULL