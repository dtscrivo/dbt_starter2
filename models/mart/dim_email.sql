{{ config(materialized='table') }}

WITH email_data AS (
  -- Step 1: Select the original columns
  SELECT 
    property_email AS email_prime,
    property_email AS email_all
  FROM 
    `bbg-platform.hubspot2.contact`

  UNION ALL

  -- Step 2: Unpivot the property_email_2 and property_hs_additional_emails columns
  SELECT
    property_email AS email_prime,
    property_email_2 AS email_all
  FROM 
    `bbg-platform.hubspot2.contact`
  WHERE 
    property_email_2 IS NOT NULL

  UNION ALL

  SELECT
    property_email AS email_prime,
    email AS email_all
  FROM 
    `bbg-platform.hubspot2.contact`,
    UNNEST(SPLIT(property_hs_additional_emails, ',')) AS email
  WHERE 
    email IS NOT NULL

  UNION ALL

  -- Step 3: Include property_email as a row in email_all
  SELECT
    property_email AS email_prime,
    property_email AS email_all
  FROM 
    `bbg-platform.hubspot2.contact`
),

-- Step 4: Exclude rows where email_all matches property_email_2 or property_hs_additional_emails
final_data AS (
  SELECT 
    email_prime,
    email_all
  FROM 
    email_data
  WHERE 
    email_all NOT IN (
      SELECT property_email_2 FROM `bbg-platform.hubspot2.contact` WHERE property_email_2 IS NOT NULL
    ) 
    AND email_all NOT IN (
      SELECT email FROM `bbg-platform.hubspot2.contact`, UNNEST(SPLIT(property_hs_additional_emails, ',')) AS email
      WHERE email IS NOT NULL
    )
  
  UNION ALL

  -- Step 5: Include all emails in email_all
  SELECT 
    property_email AS email_prime,
    email AS email_all
  FROM 
    `bbg-platform.hubspot2.contact`,
    UNNEST(SPLIT(property_hs_additional_emails, ',')) AS email
  WHERE 
    email IS NOT NULL

  UNION ALL

  SELECT
    property_email AS email_prime,
    property_email_2 AS email_all
  FROM 
    `bbg-platform.hubspot2.contact`
  WHERE 
    property_email_2 IS NOT NULL
)

-- Step 6: Select distinct email_prime and email_all pairs
SELECT 
  email_prime,
  email_all
FROM 
  final_data
GROUP BY 
  email_prime, email_all
ORDER BY 
  email_prime, email_all