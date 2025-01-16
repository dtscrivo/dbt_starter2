{{ config(
    materialized='table',
    schema='hubspot_sales'
) }}

SELECT  l.id_contact
  , pipeline_stage
  , d.email
  , CAST(l.date_lead_setter_assigned as DATE) as date_lead_setter_assigned
  , d.id_deal
  , d.name_deal
  , d.is_buyer
  , l.id_closer
  , CAST(d.date_entered_appt_scheduled as DATE) as date_entered_appt_scheduled
  , d.date_deal_created

  , l.name_owner as contact_owner
  , d.name_owner as deal_owner
  , l.name_setter as contact_setter
  , d.name_setter as deal_setter
  , l.name_closer as contact_closer
  , d.name_closer as deal_closer
FROM `dbt_tscrivo_hubspot_sales.sales_leads` l
LEFT JOIN `dbt_tscrivo.fct_hs_deal` d
  on l.id_contact = d.id_contact
 -- and d.date_closed IS NULL
  and (d.name_deal LIKE "%Phone Setter Booked Discovery Call" OR d.name_deal LIKE "%Mastermind Business Academy%")
--  and l.id_owner = d.id_owner
where pipeline = "1:1 Sales Pipeline"
 -- and CAST(l.date_lead_setter_assigned as DATE) = '2024-11-13'
 -- and l.email = 'kotsy.krisztina@gmail.com'
 -- and l.name_owner = 'R & C Team'
 -- and d.id_deal = 84717556982
GROUP BY ALL
QUALIFY ROW_NUMBER() OVER(PARTITION BY l.id_contact ORDER BY d.date_deal_created DESC) = 1

{# name or rename deal with "Phone Setter Booked Discovery Call"
# set closer and owner to who they're booked with
# setter should remain the same #}