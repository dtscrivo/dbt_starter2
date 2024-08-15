{{ config(materialized='table') }}

with hubspot as (
  WITH deal_source AS (
Select d.deal_id as id_deal
   , cast(deal_pipeline_id as string) as id_pipeline
   , cast(deal_pipeline_stage_id as string) as id_pipeline_stage
   , owner_id as id_owner
   , cast(portal_id as string) as id_portal
   , property_product_name as name_product_plantype
   , property_product_status as status_deal
   , property_dealname as name_deal
   , DATE(property_createdate) as date_created
   , property_email_address_of_contact as customer_email
   , property_initial_meeting_type as initial_meeting_type
   , property_hs_analytics_source as analytics_source
   , property_dealtype as customer_type
   , DATE(property_estimated_contract_end_date) as date_contractended
   , is_deleted as is_deleted
   , property_hs_deal_score as score
   , property_hs_is_closed as is_closed
   , property_hs_is_closed_won as is_closed_won
   , property_hs_is_deal_split as is_split
   , DATE(property_hs_lastmodifieddate)  as date_lastmodified
   , property_hs_latest_meeting_activity as latest_meeting_activity
   , concat(property_first_name_of_contact_record, " ", property_last_name_of_contact_record) as name_client
   , property_variance_score_deal_record_ as variance_score
   , property_ws_ticket_type as ticket_type
   , property_hs_num_associated_active_deal_registrations as num_active_registrations
   , property_hs_num_associated_deal_registrations as num_registrations
   , property_hs_num_associated_deal_splits as num_splits
   , property_hs_num_of_associated_line_items as num_lineitems
   , property_hs_num_target_accounts as num_target_accounts
   , property_oncehub_booking_id as id_booking
   , property_oncehub_meeting_type as meeting_type
   , property_offer_made as is_offermade
   , property_objection_reason as objection_reason
   , property_setter_name as name_setter
   , property_hs_was_imported as is_imported
   , property_payment_status as status_property_payment
   , property_contract_status as status_contract
   , case when contains_substr(property_product_name, "PIF") then "PIF" ELSE "PP" END as pay_type
   , DATE(property_hs_closed_won_date) as date_closed_won
   , property_wire_payment_ as is_wire
   , property_upgrade_deal_ as is_upgrade
   , property_transfer_info as transfer_info
   , property_offer_made as offer_made
   , property_backend_cancellation_ as is_backend_cancellation
   , DATE(property_most_recent_save_request) as date_latest_save_request
   , property_hs_closed_won_count as count_closed_won
   , property_hs_is_open_count as count_open
   , property_oncehub_meeting_type as oncehub_meeting_type
   , property_outbound_lead_source_deal_record_ as source_outbound_lead
   , property_days_to_close as days_to_close
   , property_subscription_id as id_stripe_sub
   , property_transaction_id as id_stripe_paymentintent
   , property_product_id as id_stripe_product
   , property_invoice_id as id_stripe_invoice
   , property_hs_merged_object_ids as merged_deals
   , property_decline_date as date_declined
   , property_initial_discovery_call_scheduled_for_date as date_initial_discovery_call_meeting
   , property_initial_meeting_date as date_discovery_call_meeting
   , property_initial_meeting_type_create_date as date_initial_discovery_call_booked
   , property_closedate as date_closed
   , property_hs_date_entered_292741310 as date_paused
   , property_hs_date_entered_38716614 as date_scheduled
   , property_deal_lead_source_sales_ as source_saleslead
   , property_last_reached_out
   , property_member_success_advisor as id_success_advisor
   , property_action_academy_success_path
   , property_coach as id_coach
   , property_most_recent_coaching_session as date_last_coaching_session
   , property_hubspot_owner_assigneddate as date_owner_assigned
   , property_product_id as id_product
   , property_taa_student_health_score as taa_health_score
   , property_hs_acv as amount_contract
   , property_future_contracted_value as amount_owed
   , property_save_owner as id_save_owner
   , property_hs_closed_won_count as count_closed_won_hs
   , property_taa_student_orientation_date as date_taa_orientation
   , property_hs_date_entered_63590844 as date_noshow_entered
   , property_hs_date_exited_63590844 as date_noshow_exit
   , property_coach
   , property_most_recent_coaching_session
   , property_reduce_session_
   , property_session_attendance
   , property_total_sessions_completed
   , property_credits
   , property_original_credits_purchased_testing
   , case when property_product_name = "1 × The Action Academy (at $1,637.00 / month)" then "taa_pp_nolp_1637_4"
          when property_product_name = "1 × The Action Academy (at $5,997.00 / month)" then "taa_pif_nolp_5997_1"
          when property_product_name = "The Action Academy (PIF)" then "taa_pif_nolp_5997_1"
          when property_product_name = "The Action Academy ($935.00*7)" then "taa_pp_lp_935_7"
          when property_product_name = "The Action Academy ($1637.00*4)" then "taa_pp_nolp_1637_4"
          when property_product_name = "The Action Academy (Launchpad Not Included) (4 Pay)" then "taa_pp_nolp_1637_4"
          when property_product_name = "The Action Academy TCA Dropout Or Transfer Package (PIF)" then "taa_pif_nolp_5997_1"
          when property_product_name = "Mastermind Business Academy ($1417.00*6)" then "MBA_vip_pp_1833_6"
          else property_pricing_id end as pricing_id
   , property_pricing_id
   , property_amount_in_home_currency as property_amount
FROM `bbg-platform.hubspot2.deal` d
LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
   on d.deal_id = m.merged_deal_id
WHERE m.merged_deal_id IS NULL
   and is_deleted = FALSE
qualify row_number() over (partition by property_email_address_of_contact,property_dealname order by property_closedate desc) = 1
)

, tickets AS (
SELECT t.id as id_ticket
   , t.property_createdate as date_ticket_created
   , t.property_closed_date as date_ticket_closed
   , t.property_decline_amt as amount_decline
   , t.property_disputed_amount as amount_dispute
   , t.property_email_address_of_contact as client_email
   , d.deal_id as id_deal
   , t.property_subject as ticket_subject
   , t.property_retention_status as status_retention
   , t.property_reason_for_canceled_request as cancel_reason
   , t.property_num_notes as num_notes
   , t.property_num_contacted_notes as num_contacted_notes
   , t.property_notes_last_updated as date_notes_last_updated
   , t.property_notes_last_contacted as date_notes_last_contacted
   , t.property_new_playbook_submitted as date_new_playbook_submitted
   , t.property_last_reply_date as date_last_reply
   , t.property_last_engagement_date as date_last_engagement
   , t.property_hubspot_owner_id as id_ticket_owner
   , t.property_hubspot_owner_assigneddate as date_owner_assigned 
   , t.property_hs_sales_email_last_replied as date_sales_last_replied
   , t.property_hs_pipeline as id_pipeline
   , t.property_hs_pipeline_stage as id_pipeline_stage
   , t.property_hs_last_email_activity
   , t.property_hs_last_email_date as date_last_email
FROM `bbg-platform.hubspot2.ticket` t
LEFT JOIN `bbg-platform.hubspot2.ticket_deal` d
  on t.id = d.ticket_id
WHERE true
  and t.property_hs_pipeline = 42858454 -- backend save pipeline
  and t.id <> 2840342981
qualify row_number() over (partition by d.deal_id order by t.property_createdate desc) = 1
)


, payments as (
  SELECT coalesce(cast(m.deal_id as string), json_extract_scalar(c.metadata, "$.deal_id")) as deal_id
    , sum(c.amount/100) as amount_collected
    , sum(r.amount/100) as amount_refund
  FROM `bbg-platform.stripe_mindmint.charge` c
  LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
    on cast(json_extract_scalar(c.metadata, "$.deal_id") as string) = cast(m.merged_deal_id as string)
  LEFT JOIN `bbg-platform.stripe_mindmint.refund` r
    on c.id = r.charge_id
  WHERE c.status = "succeeded"
    and json_extract_scalar(c.metadata, "$.product_id") != "1727719140"
    GROUP BY ALL
)

, clickfunnel AS(
SELECT json_extract_scalar(h.metadata, "$.netsuite_CF_funnel_id") as funnel_id
 , c.metadata
 , c.email
 , h.created
 , h.id
 , json_extract_scalar(c.metadata, "$.processor") as processor
FROM `bbg-platform.stripe_mindmint.subscription_history` h
LEFT JOIN `bbg-platform.stripe_mindmint.customer` c
  on h.customer_id = c.id
WHERE json_extract_scalar(h.metadata, "$.netsuite_CF_funnel_id") = "13216474"
 -- and json_extract_scalar(c.metadata, "$.deal_id") = "11383028672"
qualify row_number() over( partition by c.email order by h.created desc ) = 1
)

, wires as (
    SELECT deal_id
  , sum(property_amount) as property_amount
  , property_product_name
 -- , sum(property_amount_collected)
 -- , sum(property_amount_in_home_currency)
FROM `bbg-platform.hubspot2.deal`
WHERE true
  and property_wire_payment_ = TRUE
  group by all
)

, paynum AS (
  SELECT coalesce(cast(m.deal_id as string), json_extract_scalar(c.metadata, "$.deal_id")) as deal_id
    , row_number() over( partition by coalesce(cast(m.deal_id as string), json_extract_scalar(c.metadata, "$.deal_id")), c.status order by c.created asc) as num_payments
    , row_number() over( partition by coalesce(cast(m.deal_id as string), json_extract_scalar(c.metadata, "$.deal_id")), c.status order by c.created desc) as last_payment
    , c.metadata
    , c.id
    , c.created
    , c.amount
    , c.status
    , json_extract_scalar(c.metadata, "$.processor") as processor
    , r.created as date_refund
  FROM `bbg-platform.stripe_mindmint.charge` c
  LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
    on cast(json_extract_scalar(c.metadata, "$.deal_id") as string) = cast(m.merged_deal_id as string)
    LEFT JOIN `bbg-platform.stripe_mindmint.refund` r
    on c.id = r.charge_id
  WHERE c.status = "succeeded"
 --   qualify row_number() over(partition by coalesce(cast(m.deal_id as string), json_extract_scalar(metadata, "$.deal_id")), status order by created desc) = 1
)

, sub_invoice AS (
SELECT subscription_id
  , sum(amount_paid / 100) as amount_paid
  , row_number() over(partition by subscription_id, status order by created asc) as payment_num
  , created
  , status
--  , amount_paid / 100 as amount
FROM `bbg-platform.stripe_mindmint.invoice` 
where true 
   and status = "paid"
GROUP BY ALL
QUALIFY row_number() over(partition by subscription_id, status order by created desc) = 1
)

SELECT d.*
   , p.label as pipeline_stage
   , ppp.label as pipeline
   , concat(o.first_name," ",o.last_name) as name_owner
   , concat(se.first_name," ",se.last_name) as name_sa
   , concat(sa.first_name," ",sa.last_name) as name_save_owner
   , t.date_ticket_created
   , date(t.date_ticket_closed) as date_ticket_resolved
   , t.cancel_reason
   , t.status_retention
   , pp.amount_refund
 --  , hsp.property_price as amount_product_price
  , right(d.pricing_id, 1) as num_plan_payments
  , cf.funnel_id
  , case when coalesce(pn.processor, cf.processor) = "inperson" then "inperson"
         --when cf.funnel_id = "13216474" then "clickfunnel"
         when cf.funnel_id IS NOT NULL then "clickfunnel"
         when coalesce(pn.processor, cf.processor) = "gateway" then "gateway"
         else "gateway" end as payment_source
  , cf.id as id_subscription
-- , d.pricing_id
--  , d.property_amount
  , coalesce(pp.amount_collected, si.amount_paid) as amount_collected
  , coalesce(pn.num_payments, si.payment_num) as num_payments_made
  , pn.created as date_charge
  , pn2.created as date_first_charge
--  , pp.date_refunded\
  , w.property_amount as wire
  , pn.date_refund
FROM  deal_source d
LEFT JOIN `bbg-platform.hubspot2.deal_pipeline_stage` p
  ON d.id_pipeline_stage = p.stage_id
LEFT JOIN `bbg-platform.hubspot2.deal_pipeline` ppp
  ON d.id_pipeline = ppp.pipeline_id
LEFT JOIN `bbg-platform.hubspot2.owner` o
  ON d.id_owner= o.owner_id
LEFT JOIN payments pp
  ON cast(d.id_deal as string) = pp.deal_id
LEFT JOIN `bbg-platform.hubspot2.owner` se
  ON d.id_success_advisor = se.owner_id
LEFT JOIN `bbg-platform.hubspot2.owner` sa
  ON d.id_save_owner = se.owner_id
LEFT JOIN tickets t
  on d.id_deal = t.id_deal
--LEFT JOIN `bbg-platform.hubspot2.product` hsp
 -- on d.pricing_id = cast(hsp.property_pricing_id as string)
LEFT JOIN clickfunnel cf
  on d.customer_email = cf.email
LEFT JOIN paynum pn
  on cast(d.id_deal as string) = cast(pn.deal_id as string)
  and pn.last_payment = 1
LEFT JOIN paynum pn2
  on cast(d.id_deal as string) = cast(pn2.deal_id as string)
  and pn2.num_payments = 1
LEFT JOIN sub_invoice si
  on cf.id = si.subscription_id
LEFT JOIN wires w
  on d.id_deal = w.deal_id
--LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
--  on d.id_deal = m.deal_id
 -- and m.deal_id is not null
--LEFT JOIN paynum pn2 
--  on cast(m.merged_deal_id as string) = pn2.deal_id
WHERE true
--  and d.id_deal = 11311572982
   and d.is_deleted = FALSE
   and analytics.fnEmail_IsTest(customer_email) = false
)


SELECT *
  , count(*) as num_sales
  , case when date_ticket_created IS NOT NULL then 1 ELSE 0 end as num_requests
  , case when date_ticket_resolved IS NOT NULL AND status_retention = "Cancelled" then 1 else 0 end as num_cancel
  , case when date_ticket_resolved IS NOT NULL AND status_retention = "Saved" then 1 else 0 end as num_saved

-- thursday
--  , DATE_ADD(cast(date_closed as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_closed) + 7, 7) DAY) AS order_week
--  , DATE_ADD(cast(DATE(date_ticket_created) as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM DATE(date_ticket_created)) + 7, 7) DAY) AS request_received_week
--  , DATE_ADD(cast(date_ticket_resolved as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_ticket_created) + 7, 7) DAY) AS resolved_week


  , CEIL(DATE_DIFF(CURRENT_DATE(), DATE(date_closed), DAY) / 7) as age_wob
  , CEIL(DATE_DIFF(DATE(date_ticket_created), DATE(date_closed), DAY) / 7) as received_wob
  , CEIL(date_diff(date_ticket_resolved, DATE(date_ticket_created), day) / 7) as resolved_wob_since_request
  , date_diff(date(date_ticket_created), DATE(date_closed), day) as received_dob
  , CEIL(date_diff(current_date(), DATE(date_ticket_created), day) / 7) as received_age_wob
  , CEIL(date_diff(date(date_ticket_resolved), DATE(date_closed), day) / 7) as resolved_wob


  , date_diff(current_date, DATE(date_closed), day) as dob
  , date_diff(date(date_ticket_resolved), DATE(date_closed), day) as dob_resolved


  , extract(month from date_closed) as order_month
  , FORMAT_DATE('%y%m', date_closed) order_cohort_month

--friday

  , DATE_ADD(cast(date_ticket_resolved as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_ticket_resolved) + 8, 7) DAY) AS resolved_week
  , DATE_ADD(cast(date_closed as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_closed) + 8, 7) DAY) AS order_week
 -- , case when pay_type = "PIF" AND num_payments_made > 1 then 1 else 0 end as extra_pif
  , DATE_ADD(cast(date_ticket_created as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_ticket_created) + 8, 7) DAY) AS requested_week


  , CEIL(DATE_DIFF(CURRENT_DATE(), DATE('2024-03-06'), DAY) / 7) as kbb_wob
  , case when date(date_closed) > DATE('2024-01-01') then 1 else 0 end as current_year


  , case when name_product_plantype LIKE "%The Action Academy%" then "TAA" 
         when name_product_plantype LIKE "%Mastermind Business Academy%" then "MBA"
         else "" end as program

  , case when pay_type = "PIF" AND num_payments_made > 1 then 1 else 0 end as pif2
  , date_diff(date_charge, date_closed, day) as charge_dob

  , date_diff(date_first_charge, date_closed, day) as first_charge_dob

  , date_diff(date_charge,date_first_charge, day) as first_to_last_dob
  , case when payment_source = "clickfunnel" and date(date_closed) < DATE('2024-04-01') then 1 else 0 end as KBB

  , DATE_ADD(cast(date_charge as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_charge) + 8, 7) DAY) AS charge_week

  , CEIL(DATE_DIFF(CURRENT_DATE(), DATE(date_closed), DAY) / 30) as age_mob
  , CEIL(date_diff(date(date_ticket_resolved), DATE(date_closed), day) / 30) as resolved_mob

  , case when amount_refund = amount_collected then 1 else 0 end as full_refund
  , date_diff(date_refund, date_closed, day) as refund_dob
  , case when name_product_plantype LIKE "%Hybrid%" then 1 else 0 end as VIP
  , case when status_deal IN ('Active', 'Declined', 'Save Requested') then 1 else 0 end as num_active
  , case when status_deal = "Paused" then 1 else 0 end as num_paused
  , case when date_closed IS NOT NULL and pipeline_stage NOT IN ("Cancelled", "Cancelled Student", "Paused") then 1 else 0 end as active

  , case when pipeline_stage IN ('Cancelled', 'Paused Student', 'Current Declines', 'Cancelled Student', 'Closed Won')and name_deal NOT LIKE "%In-Person%" then 1 else 0 end as is_buyer
  , case when pipeline_stage IN ('Cancelled', 'Cancelled Student')and name_deal NOT LIKE "%In-Person%" then 1 else 0 end as is_cancelled
  , case when pipeline_stage IN ('Paused Student')and name_deal NOT LIKE "%In-Person%" then 1 else 0 end  as is_paused
  , case when pipeline_stage IN ('Paused Student', 'Current Declines', 'Closed Won')and name_deal NOT LIKE "%In-Person%" then 1 else 0 end as is_enrolled
  , case when pipeline_stage IN ('Current Declines', 'Closed Won')and name_deal NOT LIKE "%In-Person%" then 1 else 0 end as is_active

from hubspot h
GROUP BY ALL