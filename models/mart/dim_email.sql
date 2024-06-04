WITH contact AS (
  SELECT
    id as contact_id,
    date(property_createdate) as property_createdate,
    analytics.fnEmail(property_email) as property_email,
    analytics.fnEmail(property_email_2) as property_email_2,
    analytics.fnEmail(property_hs_additional_emails) as property_hs_additional_emails,
    property_email_status,
    is_deleted
  FROM
    `bbg-platform.hubspot2.contact`
)

SELECT
  coalesce(a.contact_id, b.contact_id, c.contact_id) as contact_id,
  coalesce(a.property_createdate, b.property_createdate, c.property_createdate) as dt,
  coalesce(a.property_email, b.property_hs_additional_emails, c.property_email_2) as email_all,
  coalesce(a.property_email, b.property_email, c.property_email) AS email_prime,
  coalesce(a.property_email_status, b.property_email_status, c.property_email_status) as status_all,
  coalesce(a.is_deleted, b.is_deleted, c.is_deleted) as is_deleted_all
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
WHERE coalesce(a.property_email, b.property_hs_additional_emails, c.property_email_2) is not null