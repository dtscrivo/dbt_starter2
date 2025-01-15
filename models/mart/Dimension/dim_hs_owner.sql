{{ config(
    materialized='table',
    schema='dimension'
) }}

  SELECT owner_id as id_owner
  , active_user_id
  , DATETIME(created_at, "America/Phoenix") as date_owner_created
  ,  concat(first_name," ",last_name) as name_full
 -- , analytics.fnEmail(email) as email
  , analytics.fnEmail(email) as email
  , first_name as name_first
  , last_name as name_last
  , is_active
  FROM `bbg-platform.hubspot2.owner`