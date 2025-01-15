{{ config(
    materialized='table',
    schema='dimension'
) }}


WITH ids_to_exclude AS (
  SELECT DISTINCT CAST(id AS INT64) AS id
  FROM `bbg-platform.hubspot2.contact`,
  UNNEST(SPLIT(REPLACE(property_hs_merged_object_ids, ';', ','), ',')) AS id
  WHERE SAFE_CAST(id AS INT64) IS NOT NULL
)
# removes merged and deleted contacts
SELECT *
FROM `bbg-platform.hubspot2.contact`
WHERE true
  AND is_deleted = FALSE
  and property_email is not null and property_email != ""
  AND id NOT IN (
    SELECT id
    FROM ids_to_exclude
  )