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
  , datetime(coalesce(t.property_hs_timestamp,n.property_hs_timestamp,m.property_hs_timestamp,ee.property_hs_timestamp, ec.property_hs_timestamp, cc.property_hs_timestamp), 'America/Phoenix') as timestamp
  , coalesce(ec.property_hs_call_direction, ee.property_hs_email_direction) as direction
  , coalesce(ee.property_hs_email_subject, m.property_hs_meeting_title,t.property_hs_task_subject, ec.property_hs_call_title) as subject
  , coalesce(ec.property_hubspot_owner_id, ee.property_hubspot_owner_id, cc.property_hubspot_owner_id, m.property_hubspot_owner_id, n.property_hubspot_owner_id, t.property_hubspot_owner_id) as id_owner
  , case when ec.property_hs_call_disposition = "73a0d17f-1163-4015-bdd5-ec830791da20" then "no answer" 
         when ec.property_hs_call_disposition = 'f240bbac-87c9-4f6e-bf70-924b57d47db7' then "connected"
         when ec.property_hs_call_disposition = 'b2cf5968-551e-4856-9783-52b3da59a7d0' then "left voicemail"
         when ec.property_hs_call_disposition = '9d9162e7-6cf3-4944-bf63-4dff82258764' then "busy"
         when ec.property_hs_call_disposition = '2a2764ef-b09f-46fa-a8ec-3d49cbf7b647' then "contact hung up"
         when ec.property_hs_call_disposition = '17b47fee-58de-441e-a44c-c6300d46f273' then "wrong number"
         when ec.property_hs_call_disposition = '7ed6cbc9-a685-425c-a734-5df0c807fafb' then "discovery call booked"
         when ec.property_hs_call_disposition = 'aebe017d-4f05-4cc7-9197-0760b5bdee14' then "follow up booked"
         when ec.property_hs_call_disposition = 'e5400998-ce41-46b5-8b04-462c3dc2cf3e' then "disqualified"
         when ec.property_hs_call_disposition = 'fd306d26-ee79-4d4a-b572-09f814c8a8f5' then "follow up offered"
         when ec.property_hs_call_disposition = 'b88923eb-6bfc-4f64-976a-7b68c9bd5ce9' then "discovery call offered"
         else ec.property_hs_call_disposition end
         as disposition
  , case when ec.property_hs_call_disposition IN ('f240bbac-87c9-4f6e-bf70-924b57d47db7','2a2764ef-b09f-46fa-a8ec-3d49cbf7b647',
                                                  '7ed6cbc9-a685-425c-a734-5df0c807fafb','aebe017d-4f05-4cc7-9197-0760b5bdee14',
                                                  'e5400998-ce41-46b5-8b04-462c3dc2cf3e','fd306d26-ee79-4d4a-b572-09f814c8a8f5',
                                                  'b88923eb-6bfc-4f64-976a-7b68c9bd5ce9')
                                                   then 1 else 0 end as is_call_connected
  , co.property_outbound_lead_source as source_lead
  , co.property_hs_lead_status as status_lead
  , (ec.property_hs_call_duration / 1000) / 60 as call_duration
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





