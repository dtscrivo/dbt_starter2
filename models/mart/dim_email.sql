{{ config(materialized='table') }}

with contact as (
  WITH ids_to_exclude AS (
  SELECT DISTINCT CAST(id AS INT64) AS id
  FROM `bbg-platform.hubspot2.contact`,
  UNNEST(SPLIT(REPLACE(property_hs_merged_object_ids, ';', ','), ',')) AS id
  WHERE SAFE_CAST(id AS INT64) IS NOT NULL
)

SELECT 
  property_email,
  property_email_status,
  id,
  property_createdate,
  is_deleted,
  _fivetran_deleted,
  property_hs_merged_object_ids
  , row_number() over(partition by property_email)
  , property_hs_additional_emails

FROM `bbg-platform.hubspot2.contact`
WHERE true
 -- and property_email IN ('drmiriamwolf@gmail.com','miriamworkman@hotmail.com', 'healthyself@vermontel.net')
  AND is_deleted = FALSE
  and property_email is not null and property_email != ""
  AND id NOT IN (
    SELECT id 
    FROM ids_to_exclude
  )

)

,
email_data AS (
  SELECT
    property_email,
    email
  FROM
    contact,
    UNNEST(SPLIT(property_hs_additional_emails, ';')) AS email
)
SELECT
  analytics.fnEmail(property_email) AS email_prime,
  analytics.fnEmail(email) AS email_all
FROM
  email_data
UNION ALL
SELECT
  analytics.fnEmail(property_email) AS email_prime,
  analytics.fnEmail(property_email) AS email_all
FROM
  contact