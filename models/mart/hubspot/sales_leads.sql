{{ config(
    materialized='table',
    schema='hubspot_sales'
) }}

select id_contact
  , CAST(date_setter_lead_assigned as DATE) as date_lead_setter_assigned
  , outbound_lead_source_assigned
  , is_reserved_lead
  , lead_score
  , lead_score_when_ols_assigned
  , DATE_ADD(date(date_setter_lead_assigned), INTERVAL MOD(13 - EXTRACT(DAYOFWEEK FROM date(date_setter_lead_assigned)), 7) DAY) AS week_assigned
  , lead_score_when_setter_assigned
  , CASE WHEN id_owner = 2039569549 then "R & C Team" 
        ELSE name_setter end as name_setter
  , name_closer
  , name_owner
  , id_closer
  , id_owner
  , id_setter
  , analytics.fnEmail(c.property_email) as email
  , date_closer_assigned
  , date_owner_assigned
from `dbt_production_hubspot_lead_deals.final_leads_present_status` l
left join `hubspot2.contact` c
  on l.id_contact = c.id
WHERE (date_setter_lead_assigned IS NOT NULL OR id_owner = 2039569549)