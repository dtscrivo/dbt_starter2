{{ config(
    materialized='table',
    schema='hubspot_sales'
) }}

SELECT id_engagement
  , email
  , dt
  , detail
  , id_owner
  , id_contact
  , id_team
  , name_owner
  , team
  , disposition
  , status
  , source
FROM `bbg-platform.dbt_production_hubspot_engagements.final_deal_engagement` f
-- LEFT JOIN `bbg-platform.dbt_tscrivo.dim_email` e
--   ON f.email = e.email_all
WHERE TRUE
  and type = 'EMAIL'
  and direction = 'OUTBOUND'
  and source IN ('CONVERSATIONS','SALES')