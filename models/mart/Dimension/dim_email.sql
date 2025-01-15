{{ config(
    materialized='table',
    schema='dimension'
) }}

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

{# WITH ids_to_exclude AS (
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
qualify row_number() over(partition by email_prime, email_all) = 1 #}




WITH stripe AS (
 with combined as (
  select id as id_customer
   , CASE
    WHEN REGEXP_CONTAINS(cu.email, r'\.(con|comm|cpm|coms|cin|come|xon|ccom|comu|vom|comio|coom|col|vcom|comin|cim|clm|comt|comc|coma|comp|ckm|comcom|xom|comd|comuc)$') THEN 
      REGEXP_REPLACE(cu.email, r'\.(con|comm|cpm|coms|cin|come|xon|ccom|comu|vom|comio|coom|col|vcom|comin|cim|clm|comt|comc|coma|comp|ckm|comcom|xom|comd|comuc)$', '.com')
    WHEN REGEXP_CONTAINS(cu.email, r'\.orgg$') THEN 
      REGEXP_REPLACE(cu.email, r'\.orgg$', '.org')
    WHEN REGEXP_CONTAINS(cu.email, r'\.(ner|nyt|nwt|ney)$') THEN 
      REGEXP_REPLACE(cu.email, r'\.(ner|nyt|nwt|ney)$', '.net')
    WHEN REGEXP_CONTAINS(cu.email, r'\.(co.um|co.uj|co.un)$') THEN 
      REGEXP_REPLACE(cu.email, r'\.(co.um|co.uj|co.un)$', '.co.uk')
    WHEN REGEXP_CONTAINS(cu.email, r'\@hot.ailcom$') THEN 
      REGEXP_REPLACE(cu.email, r'\@hot.ailcom$', '@hotmail.com')
    WHEN REGEXP_CONTAINS(cu.email, r'\.dee$') THEN 
      REGEXP_REPLACE(cu.email, r'\.dee$', '.de')
    WHEN REGEXP_CONTAINS(cu.email, r'\@gmailcom$') THEN 
      REGEXP_REPLACE(cu.email, r'\@gmailcom$', '@gmail.com')
    WHEN REGEXP_CONTAINS(cu.email, r'[\w.%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') THEN 
    REGEXP_EXTRACT(cu.email, r'[\w.%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
    ELSE 
      cu.email end as email

  from `bbg-platform.stripe_mastermind.customer` cu


UNION ALL

  select id as id_customer
   , CASE
    WHEN REGEXP_CONTAINS(cu.email, r'\.(con|comm|cpm|coms|cin|come|xon|ccom|comu|vom|comio|coom|col|vcom|comin|cim|clm|comt|comc|coma|comp|ckm|comcom|xom|comd|comuc)$') THEN 
      REGEXP_REPLACE(cu.email, r'\.(con|comm|cpm|coms|cin|come|xon|ccom|comu|vom|comio|coom|col|vcom|comin|cim|clm|comt|comc|coma|comp|ckm|comcom|xom|comd|comuc)$', '.com')
    WHEN REGEXP_CONTAINS(cu.email, r'\.orgg$') THEN 
      REGEXP_REPLACE(cu.email, r'\.orgg$', '.org')
    WHEN REGEXP_CONTAINS(cu.email, r'\.(ner|nyt|nwt|ney)$') THEN 
      REGEXP_REPLACE(cu.email, r'\.(ner|nyt|nwt|ney)$', '.net')
    WHEN REGEXP_CONTAINS(cu.email, r'\.(co.um|co.uj|co.un)$') THEN 
      REGEXP_REPLACE(cu.email, r'\.(co.um|co.uj|co.un)$', '.co.uk')
    WHEN REGEXP_CONTAINS(cu.email, r'\@hot.ailcom$') THEN 
      REGEXP_REPLACE(cu.email, r'\@hot.ailcom$', '@hotmail.com')
    WHEN REGEXP_CONTAINS(cu.email, r'\.dee$') THEN 
      REGEXP_REPLACE(cu.email, r'\.dee$', '.de')
    WHEN REGEXP_CONTAINS(cu.email, r'[\w.%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') THEN 
    REGEXP_EXTRACT(cu.email, r'[\w.%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
    ELSE 
      cu.email end as email
  from `bbg-platform.stripe_mindmint.customer` cu
 )

 SELECT analytics.fnEmail(email) as email
 FROM COMBINED
)

, hubspot AS (
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


Select analytics.fnEmail(email_prime) as email_prime
  , analytics.fnEmail(email_all) as email_all
FROM base
qualify row_number() over(partition by email_prime, email_all) = 1
)

SELECT COALESCE(h.email_prime, s.email, email_all) as email_prime
  , COALESCE(case when h.email_all = "" then null else h.email_all end, s.email, h.email_prime) as email_all
 -- , case when COALESCE(h.email_prime, s.email) = COALESCE(h.email_all, s.email) then 1 else 0 end
FROM stripe s
FULL JOIN hubspot h
  on s.email = h.email_all
 WHERE (COALESCE(h.email_prime, s.email, email_all) IS NOT NULL AND COALESCE(case when h.email_all = "" then null else h.email_all end, s.email, h.email_prime) IS NOT NULL)