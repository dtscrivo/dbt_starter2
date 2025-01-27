{{ config(materialized='table') }}

-- payment_source CTE not in use
with payment_source as (
  SELECT  
  h.id_deal
  , sum(pi.amount / 100) as amount
  , sum(pi.amount_received / 100) as amount_received
  , sum(c.amount_refunded / 100) as amount_refunded
, sum(case when pi.status = 'succeeded' then 1 else 0 end) as payments
  , pi.id
  , pi.status

FROM `bbg-platform.stripe_mindmint.payment_intent` pi
LEFT JOIN `bbg-platform.dbt_tscrivo.Hubspot` h
   on cast(h.id_deal as string) = JSON_EXTRACT_SCALAR(pi.metadata, '$.deal_id')
LEFT JOIN `bbg-platform.stripe_mindmint.charge` c
   on pi.id = c.payment_intent_id
LEFT JOIN `bbg-platform.stripe_mindmint.refund` r
   on pi.id = r.payment_intent_id
WHERE true
   and JSON_EXTRACT_SCALAR(pi.metadata, '$.deal_id') IS NOT NULL
 -- and h.id_deal = 10944514527
  and pi.status = "succeeded"
 -- and pi.id = "pi_3Ogab2LYbD2uWeLi0a16Ss6Y"
GROUP BY 1,6,7
)


-- most recent ticket note
, notes as (
SELECT n.property_hs_body_preview as note_preview  
   , REGEXP_EXTRACT(n.property_hs_body_preview, r'Notes: ([^:]+): ') AS note
   , n.property_hs_timestamp
   , t.ticket_id
   , row_number() over (partition by t.ticket_id order by n.property_hs_timestamp asc) as num_notes_engagement
FROM `bbg-platform.hubspot2.engagement_note` n
LEFT JOIN `bbg-platform.hubspot2.ticket_engagement` t
  on n.engagement_id = t.engagement_id
--WHERE t.ticket_id = 1898211262
qualify row_number() over (partition by t.ticket_id order by n.property_hs_timestamp desc) = 1
)

SELECT t.id as id_ticket
   , datetime(t.property_createdate, 'America/Phoenix') as date_ticket_created
   , datetime(t.property_closed_date, 'America/Phoenix') as date_ticket_closed
   , t.property_decline_amt as amount_decline
   , t.property_disputed_amount as amount_dispute
 --  , t.property_email_address_of_contact as client_email
   , d.deal_id as id_deal
   , t.property_subject as ticket_subject
   , t.property_retention_status as status_retention
   , t.property_reason_for_canceled_request as cancel_reason
   , t.property_num_notes as num_notes
 --  , t.property_num_contacted_notes as num_contacted_notes
 --  , t.property_notes_last_updated as date_notes_last_updated
 --  , t.property_notes_last_contacted as date_notes_last_contacted
 --  , t.property_new_playbook_submitted as date_new_playbook_submitted
 --  , t.property_last_reply_date as date_last_reply
 --  , t.property_last_engagement_date as date_last_engagement
   , t.property_hubspot_owner_id as id_ticket_owner
   , t.property_hubspot_owner_assigneddate as date_ticket_owner_assigned 
 --  , t.property_hs_sales_email_last_replied as date_sales_last_replied
   , t.property_hs_pipeline as id_pipeline
   , t.property_hs_pipeline_stage as id_pipeline_stage
 --  , t.property_hs_last_email_activity
 --  , t.property_hs_last_email_date as date_last_email
   , concat(o.first_name, " ", o.last_name) as ticket_owner
   , ps.label as ticket_pipeline_stage
   , p.label as ticket_pipeline
   , n.num_notes_engagement
   , n.note_preview
   , h.property_closedate as date_closed
   , concat(property_first_name_of_contact_record, " ", property_last_name_of_contact_record) as name_client
   , h.property_product_name as name_product
   , property_future_contracted_value as amount_owed
   , property_amount_in_home_currency as amount_collected
--, sum(case when pp.status = 'succeeded' then 1 else 0 end) as num_success_payments
   , h.property_hs_acv as amount_contract
--   , pp.amount_received
 --  , pp.amount_refunded
 --  , pp.payments
   , concat(se.first_name," ",se.last_name) as name_sa
   , analytics.fnEmail(h.property_email_address_of_contact) as email
   , ROW_NUMBER() OVER (
    PARTITION BY h.deal_id, t.property_hs_pipeline 
    ORDER BY 
        CASE 
            WHEN t.property_closed_date IS NULL THEN 1 
            ELSE 0 
        END,
        t.property_createdate DESC, 
        t.property_closed_date ASC
) AS recency
   , row_number() over(partition by h.deal_id,t.property_hs_pipeline order by t.property_createdate asc) as ticket_number
   , concat(r.first_name," ",r.last_name) as name_sales_rep
FROM `bbg-platform.hubspot2.ticket` t
LEFT JOIN `bbg-platform.hubspot2.ticket_deal` d
  on t.id = d.ticket_id
LEFT JOIN `bbg-platform.hubspot2.owner` o
  on t.property_hubspot_owner_id = o.owner_id
LEFT JOIN `bbg-platform.hubspot2.ticket_pipeline_stage` ps
  on cast(t.property_hs_pipeline_stage as string) = ps.stage_id
LEFT JOIN `bbg-platform.hubspot2.ticket_pipeline` p
  on cast(t.property_hs_pipeline as string) = p.pipeline_id
LEFT JOIN notes n
  on t.id = n.ticket_id
LEFT JOIN `bbg-platform.hubspot2.deal` h
  on d.deal_id = h.deal_id
LEFT JOIN `bbg-platform.hubspot2.owner` se
  ON h.property_member_success_advisor = se.owner_id
LEFT JOIN `bbg-platform.hubspot2.owner` r
  ON h.owner_id = r.owner_id
--LEFT JOIN payment_source pp
--on cast(h.property_payment_intent_id as string) = cast(pp.id as string)
--  on d.deal_id = pp.id_deal
WHERE true
  and h.deal_id IS NOT NULL
  and t._fivetran_deleted = false
 -- and d.deal_id = 12070154231
  --and p.label = "Backend Saves"
 -- and d.deal_id = 9642643135
 --  and t.property_email_address_of_contact = "rdiamond@maxibrace.com"