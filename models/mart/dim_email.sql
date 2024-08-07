{{ config(materialized='table') }}

WITH contact AS (
  SELECT
    lower(analytics.fnEmail(property_email)) AS property_email,
    lower(analytics.fnEmail(property_email_2)) AS property_email_2,
    lower(analytics.fnEmail(property_hs_additional_emails)) AS property_hs_additional_emails
  FROM
    `bbg-platform.hubspot2.contact`
)

, combined_contacts AS (
  SELECT
    COALESCE(b.property_hs_additional_emails, c.property_email_2, a.property_email) AS email_all,
    COALESCE(a.property_email, b.property_email, c.property_email) AS email_prime
  FROM
    contact a
  FULL OUTER JOIN
    contact b
  ON
    a.property_hs_additional_emails = b.property_email
  FULL OUTER JOIN
    contact c
  ON
    a.property_email = c.property_email
  WHERE
    COALESCE(a.property_email, b.property_hs_additional_emails, c.property_email_2) IS NOT NULL
)

, numbered_contacts AS (
  SELECT
    email_all,
    email_prime,
    ROW_NUMBER() OVER (PARTITION BY email_all, email_prime ORDER BY email_all) AS email_all_number
  FROM
    combined_contacts
)

SELECT
  email_all,
  email_prime,
       ROW_NUMBER() OVER (PARTITION BY email_prime ORDER BY email_all) AS email_number
FROM
  numbered_contacts
WHERE TRUE
--  AND email_prime = "amanda.l.mahaffey@gmail.com"
  AND email_all_number = 1
ORDER BY email_prime