{{ config(materialized='table') }}

{# with contact as (
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
  contact #}

WITH ids_to_exclude AS (
  SELECT DISTINCT CAST(id AS INT64) AS id
  FROM `bbg-platform.hubspot2.contact`,
  UNNEST(SPLIT(REPLACE(property_hs_merged_object_ids, ';', ','), ',')) AS id
  WHERE SAFE_CAST(id AS INT64) IS NOT NULL
)

, base as (
WITH emails AS (
  WITH example_data AS (
    SELECT property_hs_additional_emails AS email_string,
           analytics.fnEmail(property_email) as property_email
    FROM `hubspot2.contact`
    WHERE true
  AND is_deleted = FALSE
  and property_email is not null and property_email != ""
  AND id NOT IN (
    SELECT id 
    FROM ids_to_exclude
  )
  
  )
  SELECT
    property_email,
    SPLIT(email_string, ";")[SAFE_OFFSET(0)] AS email_1,
    SPLIT(email_string, ";")[SAFE_OFFSET(1)] AS email_2,
    SPLIT(email_string, ";")[SAFE_OFFSET(2)] AS email_3,
    SPLIT(email_string, ";")[SAFE_OFFSET(3)] AS email_4,
    SPLIT(email_string, ";")[SAFE_OFFSET(4)] AS email_5,
    SPLIT(email_string, ";")[SAFE_OFFSET(5)] AS email_6,
    SPLIT(email_string, ";")[SAFE_OFFSET(6)] AS email_7,
    SPLIT(email_string, ";")[SAFE_OFFSET(7)] AS email_8,
    SPLIT(email_string, ";")[SAFE_OFFSET(8)] AS email_9
  FROM example_data
)

SELECT property_email as email_prime, property_email AS email_all FROM emails
UNION ALL
SELECT property_email as email_prime, email_1 AS email_all FROM emails WHERE email_1 IS NOT NULL
UNION ALL
SELECT property_email, email_2 AS email FROM emails WHERE email_2 IS NOT NULL
UNION ALL
SELECT property_email, email_3 AS email FROM emails WHERE email_3 IS NOT NULL
UNION ALL
SELECT property_email, email_4 AS email FROM emails WHERE email_4 IS NOT NULL
UNION ALL
SELECT property_email, email_5 AS email FROM emails WHERE email_5 IS NOT NULL
UNION ALL
SELECT property_email, email_6 AS email FROM emails WHERE email_6 IS NOT NULL
UNION ALL
SELECT property_email, email_7 AS email FROM emails WHERE email_7 IS NOT NULL
UNION ALL
SELECT property_email, email_8 AS email FROM emails WHERE email_8 IS NOT NULL
UNION ALL
SELECT property_email, email_9 AS email FROM emails WHERE email_9 IS NOT NULL
)

Select *
FROM base
qualify row_number() over(partition by email_prime, email_all) = 1