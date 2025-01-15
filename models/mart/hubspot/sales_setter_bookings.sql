{{ config(
    materialized='table',
    schema = 'hubspot_engagements'
) }}

SELECT f.*
  , DATE(f.dt) as date_activity
  , COALESCE(m.property_hs_activity_type,
    REGEXP_EXTRACT(detail, r'Meeting type\s*([^\n\r]+)')) as type_meeting_activity
  , m.property_hs_meeting_location
  , l.date_setter_lead_assigned
  , TRIM(REGEXP_EXTRACT(detail, r'Setter:\s*([^,]+)')) AS name_setter_booking
  , TRIM(REGEXP_EXTRACT(detail, r'Booking ID:\s*([^\n\r]+)')) AS id_oncehub_booking
  , b.status_meeting
  , f.date_created as date_booking
FROM {{ ref('final_deal_engagement') }} f
LEFT JOIN {{ source('hubspot2', 'engagement_meeting') }} m
  on f.id_engagement = m.engagement_id
LEFT JOIN `bbg-platform.dbt_production_hubspot_lead_deals.final_leads_present_status` l
  on f.id_contact = l.id_contact
LEFT JOIN `bbg-platform.dbt_production_hubspot_lead_deals.final_leads_present_status` b
  on REGEXP_EXTRACT(detail, r'Booking ID:\s*([^\n\r]+)') = b.id_booking
where TRUE
  and f.type = "MEETING"
  and REGEXP_EXTRACT(detail, r'Setter:\s*([^,]+)') IS NOT NULL