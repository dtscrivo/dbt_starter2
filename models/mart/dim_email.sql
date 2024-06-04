-- models/contact_model.sql

{{ config(materialized='table') }}

WITH contact AS (
  SELECT
    id AS contact_id,
    DATE(property_createdate) AS property_createdate,
    analytics.fnEmail(property_email) AS property_email,
    analytics.fnEmail(property_email_2) AS property_email_2,
    analytics.fnEmail(property_hs_additional_emails) AS property_hs_additional_emails,
    property_email_status,
    is_deleted
  FROM
    {{ ref('hubspot2_contact') }}
)

SELECT
  COALESCE(a.contact_id, b.contact_id, c.contact_id) AS contact_id,
  COALESCE(a.property_createdate, b.property_createdate, c.property_createdate) AS dt,
  COALESCE(a.property_email, b.property_hs_additional_emails, c.property_email_2) AS email_all,
  COALESCE(a.property_email, b.property_email, c.property_email) AS email_prime,
  COALESCE(a.property_email_status, b.property_email_status, c.property_email_status) AS status_all,
  COALESCE(a.is_deleted, b.is_deleted, c.is_deleted) AS is_deleted_all
FROM
  contact a
FULL OUTER JOIN
  contact b
ON
  a.property_email = b.property_hs_additional_emails
FULL OUTER JOIN
  contact c
ON
  a.property_email = c.property_email_2
WHERE
  COALESCE(a.property_email, b.property_hs_additional_emails, c.property_email_2) IS NOT NULL
