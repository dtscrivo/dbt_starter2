-- aggregate table of all Hubspot engagement tables
--(ie. call, communication, email, meeting, note, task)

{{ config(materialized='table') }}

SELECT  e.id as id_engagement
  , d.deal_id as id_deal
  , c.contact_id as id_contact
  , e.type
  , co.property_email as email
  , coalesce(n.property_hs_body_preview, t.property_hs_body_preview, m.property_hs_meeting_body, ee.property_hs_body_preview, ec.property_hs_body_preview, IF(
    ARRAY_LENGTH(SPLIT(cc.property_hs_body_preview, 'Message:')) >= 2,
    SPLIT(cc.property_hs_body_preview, 'Message:')[OFFSET(1)],
    NULL
  )) AS detail
  , coalesce(t.property_hs_timestamp,n.property_hs_timestamp,m.property_hs_timestamp,ee.property_hs_timestamp, ec.property_hs_timestamp, cc.property_hs_timestamp) as timestamp
  , coalesce(ec.property_hs_call_direction, ee.property_hs_email_direction) as direction
  , coalesce(ee.property_hs_email_subject, m.property_hs_meeting_title,t.property_hs_task_subject, ec.property_hs_call_title) as subject
  , coalesce(ec.property_hubspot_owner_id, ee.property_hubspot_owner_id, cc.property_hubspot_owner_id, m.property_hubspot_owner_id, n.property_hubspot_owner_id, t.property_hubspot_owner_id) as id_owner
  , ec.property_hs_call_disposition as disposition
  , case when ec.property_hs_call_disposition = "f240bbac-87c9-4f6e-bf70-924b57d47db7" then 1 else 0 end as call_connected
  , co.property_outbound_lead_source as source_lead
  , co.property_hs_lead_status as status_lead
FROM `bbg-platform.hubspot2.engagement` e
LEFT JOIN `bbg-platform.hubspot2.engagement_email` ee
  on e.id = ee.engagement_id
LEFT JOIN  `bbg-platform.hubspot2.engagement_call` ec
  on e.id = ec.engagement_id
LEFT JOIN  `bbg-platform.hubspot2.engagement_communication` cc
  on e.id = cc.engagement_id
LEFT JOIN  `bbg-platform.hubspot2.engagement_meeting` m
  on e.id = m.engagement_id
LEFT JOIN  `bbg-platform.hubspot2.engagement_note` n
  on e.id = n.engagement_id
LEFT JOIN  `bbg-platform.hubspot2.engagement_task` t
  on e.id = t.engagement_id
LEFT JOIN  `bbg-platform.hubspot2.engagement_deal` d
  on e.id = d.engagement_id
LEFT JOIN  `bbg-platform.hubspot2.engagement_contact` c
  on e.id = c.engagement_id
LEFT JOIN  `bbg-platform.hubspot2.contact` co
  on c.contact_id = co.id
--ORDER BY d.deal_id desc, coalesce(ee.property_hs_timestamp, ec.property_hs_timestamp, cc.property_hs_timestamp) desc