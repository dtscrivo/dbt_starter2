{{ config(materialized='table') }}

with hubspot as (
  WITH deal_source AS (
Select d.deal_id as id_deal
   , p.label as pipeline_stage
   , ppp.label as pipeline
   , concat(o.first_name," ",o.last_name) as name_owner
   , concat(se.first_name," ",se.last_name) as name_success_advisor
   , concat(sa.first_name," ",sa.last_name) as name_save_owner
 --  , cast(portal_id as string) as id_portal
   , property_product_name as name_product
   , property_product_status as status_deal
   , property_dealname as name_deal
   , DATE(d.property_createdate) as date_deal_created
   , analytics.fnEmail(property_email_address_of_contact) as email
   , property_initial_meeting_type as initial_meeting_type
   , property_dealtype as type_deal
 --  , DATE(property_estimated_contract_end_date) as date_contractended
 --  , property_hs_is_closed as is_closed
 --  , property_hs_is_closed_won as is_closed_won
   , property_hs_is_deal_split as is_split
   , DATE(d.property_hs_lastmodifieddate)  as date_lastmodified
--   , property_hs_latest_meeting_activity as latest_meeting_activity
   , concat(property_first_name_of_contact_record, " ", property_last_name_of_contact_record) as name_client
 --  , property_ws_ticket_type as ticket_type
 --  , property_hs_num_associated_deal_splits as num_splits
   , property_hs_num_of_associated_line_items as num_lineitems
   , property_oncehub_booking_id as id_booking
   , property_oncehub_meeting_type as meeting_type
   , d.property_offer_made as is_offermade
   , property_objection_reason as objection_reason
--   , property_hs_was_imported as is_imported
   , property_payment_status as status_payment
--   , property_contract_status as status_contract
   , case when contains_substr(property_product_name, "PIF") then "PIF" ELSE "PP" END as pay_type
   , property_wire_payment_ as is_wire
   , property_upgrade_deal_ as is_upgrade
   , property_transfer_info as transfer_info
   , d.property_offer_made as offer_made
 --  , property_backend_cancellation_ as is_backend_cancellation
   , DATE(property_most_recent_save_request) as date_latest_save_request
   , property_oncehub_meeting_type as oncehub_meeting_type
   , property_outbound_lead_source_deal_record_ as source_outbound_lead
   , property_subscription_id as id_hs_subscription
   , property_transaction_id as id_hs_paymentintent
   , property_product_id as id_stripe_product
   , property_invoice_id as id_hs_invoice
   , d.property_hs_merged_object_ids as merged_deals
   , property_decline_date as date_declined
   , property_initial_discovery_call_scheduled_for_date as date_initial_discovery_call_meeting
   , property_initial_meeting_date as date_discovery_call_meeting
   , property_initial_meeting_type_create_date as date_initial_discovery_call_booked
   , DATETIME(property_closedate, 'America/Phoenix') as date_closed
   , case when DATE(property_hs_date_entered_292741310) != DATE(property_hs_date_exited_292741310) OR (property_hs_date_entered_292741310 is not null and property_hs_date_exited_292741310 is null)
     then property_hs_date_entered_292741310 else null end as date_paused
   , case when DATE(property_hs_date_entered_292741310) != DATE(property_hs_date_exited_292741310) OR (property_hs_date_entered_292741310 is not null and property_hs_date_exited_292741310 is null) then property_hs_date_exited_292741310 else null end as date_unpaused
   , property_hs_date_entered_38716614 as date_scheduled
   , property_deal_lead_source_sales_ as source_saleslead
   , property_last_reached_out
--   , property_action_academy_success_path
 --  , d.property_hubspot_owner_assigneddate as date_owner_assigned
--   , property_product_id as id_product
   , property_hs_acv as amount_contract
--   , property_future_contracted_value as amount_owed
--   , property_save_owner as id_save_owner
--   , property_taa_student_orientation_date as date_taa_orientation
   , property_hs_date_entered_63590844 as date_noshow_entered
   , property_hs_date_exited_63590844 as date_noshow_exit
   , case when property_hs_date_entered_63590844 is not null and (property_hs_date_exited_63590844 is null OR property_hs_date_entered_63590844 != property_hs_date_exited_63590844) then 1 else 0 end as is_noshow
   , case when property_product_name = "1 × The Action Academy (at $1,637.00 / month)" then "taa_pp_nolp_1637_4"
          when property_product_name = "1 × The Action Academy (at $5,997.00 / month)" then "taa_pif_nolp_5997_1"
          when property_product_name = "The Action Academy (PIF)" then "taa_pif_nolp_5997_1"
          when property_product_name = "The Action Academy ($935.00*7)" then "taa_pp_lp_935_7"
          when property_product_name = "The Action Academy ($1637.00*4)" then "taa_pp_nolp_1637_4"
          when property_product_name = "The Action Academy (Launchpad Not Included) (4 Pay)" then "taa_pp_nolp_1637_4"
          when property_product_name = "The Action Academy TCA Dropout Or Transfer Package (PIF)" then "taa_pif_nolp_5997_1"
          when property_product_name = "Mastermind Business Academy ($1417.00*6)" then "MBA_vip_pp_1833_6"
          else property_pricing_id end as id_price
   , property_pricing_id
--   , property_amount_in_home_currency as amount_hs
   , d.owner_id as id_owner
   , cast(deal_pipeline_id as string) as id_pipeline
   , cast(deal_pipeline_stage_id as string) as id_pipeline_stage
   , property_member_success_advisor as id_success_advisor
   , date(c.property_mba_orientation_date) as date_mba_orientation
   , c.id as id_contact
   , d.property_amount as amount_hubspot
   , d.property_test_setter_user_name as id_setter
   , concat(sn.first_name," ",sn.last_name) as name_setter
   , c.property_mm_lead_score as lead_score
   , c.property_state as home_state
   , c.property_ip_state as home_state_ip
  , coalesce(c.property_closer_contact_owner_assigned_date, d.property_hubspot_owner_assigneddate) as date_owner_assigned
  , c.property_setter_contact_owner_assigned_date as date_setter_assigned
  , c.property_hubspot_owner_assigneddate
  , c.property_most_recent_date_assigned_outbound_lead_source as date_lead_assigned
  , DATETIME(property_hs_date_entered_38716614, 'America/Phoenix') as date_entered_appt_scheduled
  , c.property_hs_timezone
  , c.property_hs_ip_timezone
  , d.property_hubspot_team_id as id_team
  , c.property_closer_contact_owner as id_closer
FROM `bbg-platform.hubspot2.deal` d
LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
   on d.deal_id = m.merged_deal_id
LEFT JOIN `bbg-platform.hubspot2.deal_pipeline_stage` p
  ON cast(d.deal_pipeline_stage_id as string) = p.stage_id
LEFT JOIN `bbg-platform.hubspot2.deal_pipeline` ppp
  ON cast(d.deal_pipeline_id as string) = ppp.pipeline_id
LEFT JOIN `bbg-platform.hubspot2.owner` o
  ON d.owner_id = o.owner_id
LEFT JOIN `bbg-platform.hubspot2.owner` se
  ON d.property_member_success_advisor = se.owner_id
LEFT JOIN `bbg-platform.hubspot2.owner` sa
  ON d.property_save_owner = sa.owner_id
LEFT JOIN `bbg-platform.hubspot2.owner` sn
  ON d.property_test_setter_user_name = sn.owner_id
LEFT JOIN `hubspot2.deal_contact` dc
  ON d.deal_id = dc.deal_id
LEFT JOIN `hubspot2.contact` c
  ON dc.contact_id = c.id
WHERE m.merged_deal_id IS NULL
   and d.is_deleted = FALSE
qualify row_number() over (partition by property_email_address_of_contact,property_dealname order by property_closedate desc) = 1
)

-- -- , tickets AS (
-- -- SELECT 
-- -- FROM `bbg-platform.dbt_tscrivo.fct_hs_tickets` t
-- -- where recency = 1

-- WHERE true
--   and t.id_pipeline = 42858454 -- backend save pipeline
--   and t.id_ticket <> 2840342981
-- qualify row_number() over (partition by d.deal_id order by t.property_createdate desc) = 1
-- )


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
SELECT json_extract_scalar(h.metadata, "$.netsuite_CF_funnel_id") as id_funnel
 , c.metadata
 , c.email
 , h.created
 , h.id
 , json_extract_scalar(c.metadata, "$.processor") as processor
FROM `bbg-platform.stripe_mindmint.subscription_history` h
LEFT JOIN `bbg-platform.stripe_mindmint.customer` c
  on h.customer_id = c.id
--WHERE json_extract_scalar(h.metadata, "$.netsuite_CF_funnel_id") = "13216474"
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
   , t.date_ticket_created
   , date(t.date_ticket_closed) as date_ticket_resolved
   , t.cancel_reason
   , t.status_retention
   , pp.amount_refund
 --  , hsp.property_price as amount_product_price
  , case when lower(d.id_price) like "%affirm%" then '1' else right(d.id_price, 1) end as num_plan_payments
  , cf.id_funnel
  , case when coalesce(pn.processor, cf.processor) = "inperson" then "inperson"
         --when cf.funnel_id = "13216474" then "clickfunnel"
         when cf.id_funnel IS NOT NULL then "clickfunnel"
         when coalesce(pn.processor, cf.processor) = "gateway" then "gateway"
         else "gateway" end as payment_source
  , cf.id as id_subscription
-- , d.id_price
-- , d.property_amount
  , coalesce(pp.amount_collected, si.amount_paid) as amount_collected
  , coalesce(pn.num_payments, si.payment_num) as num_payments_made
  , pn.created as date_charge
  , pn2.created as date_first_charge
--  , pp.date_refunded\
  , w.property_amount as wire
  , pn.date_refund
  , t.id_ticket
  , t.ticket_pipeline
  , t.ticket_pipeline_stage

FROM  deal_source d
LEFT JOIN payments pp
  ON cast(d.id_deal as string) = pp.deal_id
LEFT JOIN `bbg-platform.dbt_tscrivo.fct_hs_tickets` t
  on d.id_deal = t.id_deal
  and t.recency = 1
  and t.id_pipeline = 42858454 -- backend save pipeline
--LEFT JOIN `bbg-platform.hubspot2.product` hsp
 -- on d.id_price = cast(hsp.property_pricing_id as string)
LEFT JOIN clickfunnel cf
  on d.id_hs_subscription = cf.id
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
   and (analytics.fnEmail_IsTest(d.email) = false or d.email = 'chrisj@remotestaff.com')
)


SELECT h.*
--  , count(*) as num_sales
  , case when date_ticket_created IS NOT NULL then 1 ELSE 0 end as is_requests
  , case when date_ticket_resolved IS NOT NULL AND ticket_pipeline_stage = "Cancelled" then 1 else 0 end as is_cancel
  , case when date_ticket_resolved IS NOT NULL AND ticket_pipeline_stage = "Saved" then 1 else 0 end as is_saved

-- thursday
--  , DATE_ADD(cast(date_closed as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_closed) + 7, 7) DAY) AS order_week
--  , DATE_ADD(cast(DATE(date_ticket_created) as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM DATE(date_ticket_created)) + 7, 7) DAY) AS request_received_week
--  , DATE_ADD(cast(date_ticket_resolved as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_ticket_created) + 7, 7) DAY) AS resolved_week


  , CEIL(DATE_DIFF(CURRENT_DATE(), DATE(date_closed), DAY) / 7) as wob_deal_age
  , CEIL(DATE_DIFF(DATE(date_ticket_created), DATE(date_closed), DAY) / 7) as wob_ticket_received
  , CEIL(date_diff(date_ticket_resolved, DATE(date_ticket_created), day) / 7) as wob_ticket_time_to_resolve
  , CEIL(date_diff(date(date_ticket_resolved), DATE(date_closed), day) / 7) as wob_ticket_resolved_from_sale
  , CEIL(DATE_DIFF(CURRENT_DATE(), DATE(date_ticket_created), DAY) / 7) as wob_ticket_age

  , date_diff(current_date, DATE(date_closed), day) as dob_deal_age
  , date_diff(date(date_ticket_resolved), DATE(date_closed), day) as dob_ticket_resolved
  , date_diff(date(date_ticket_resolved), DATE(date_ticket_created), day) as dob_ticket_time_to_resolve
  , date_diff(date(date_ticket_created), DATE(date_closed), day) as dob_ticket_received
  , date_diff(current_date, DATE(date_ticket_created), day) as dob_ticket_age
  , extract(month from date_closed) as order_month
  , FORMAT_DATE('%y%m', date_closed) order_cohort_month

--friday

  , DATE_ADD(cast(date_ticket_resolved as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_ticket_resolved) + 8, 7) DAY) AS week_resolved
  , DATE_ADD(cast(date_closed as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_closed) + 8, 7) DAY) AS week_order
 -- , case when pay_type = "PIF" AND num_payments_made > 1 then 1 else 0 end as extra_pif
  , DATE_ADD(cast(date_ticket_created as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_ticket_created) + 8, 7) DAY) AS week_requested


  , CEIL(DATE_DIFF(CURRENT_DATE(), DATE('2024-03-06'), DAY) / 7) as wob_kbb
  , case when extract(year from date(date_closed)) = extract(year from CURRENT_DATE) then 1 else 0 end as is_current_year


  , case when name_product LIKE "%The Action Academy%" then "TAA" 
         when name_product LIKE "%Launch Lab%" OR name_product LIKE "%In-Person%" THEN "LL"
         when name_product LIKE "%Mastermind Business Academy%" then "MBA"
         when name_product LIKE "%1:1 Coaching%" then "1:1"
         when name_product LIKE "%The Edge%" then "Edge"
         when name_product LIKE "%The Coaching Academy%" then "TCA"
         when name_product LIKE "%High Ticket Coaching%" then "HTC"
         when name_product LIKE "%High Ticket Academy%" then "HTA"
         when name_product LIKE "%Zenith%" then "Zenith"

         else "" end as program

  , case when pay_type = "PIF" AND num_payments_made > 1 then 1 else 0 end as pif2
  , date_diff(datetime(date_charge), date_closed, day) as dob_charge

  , date_diff(datetime(date_first_charge), date_closed, day) as dob_first_charge

  , date_diff(date_charge,date_first_charge, day) as first_to_last_dob

  -- EVENTS
  , case when payment_source = "clickfunnel" and date(date_closed) < DATE('2024-04-01') then "KBB" 
         when id_owner = 1426033370 and FORMAT_DATE('%y%m', date_closed) = '2408' then "ws24"
         when id_owner = 1426033370 and FORMAT_DATE('%y%m', date_closed) = '2412' then "bbw24"
         else null end as event

  , DATE_ADD(cast(date_charge as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_charge) + 8, 7) DAY) AS week_charge

  , CEIL(DATE_DIFF(CURRENT_DATE(), DATE(date_closed), DAY) / 30) as mob_deal_age
  , CEIL(date_diff(date(date_ticket_resolved), DATE(date_closed), day) / 30) as mob_resolved


  , date_diff(datetime(date_refund), date_closed, day) as dob_refund
  , case when name_product LIKE "%Hybrid%" then 1 else 0 end as VIP

  , case when lower(pipeline_stage) IN ('cancelled', 'paused student', 'current declines', 'cancelled student', 'closed won','mba membership completed','active retention save deal','active','mba deal completed') and date_closed is not null then 1 else 0 end as is_buyer
  , case when lower(pipeline_stage) LIKE '%cancelled%' then 1 else 0 end as is_cancelled
  , case when lower(pipeline_stage) IN ('paused student') then 1 else 0 end  as is_paused
  , case when lower(pipeline_stage) IN ('paused student', 'current declines', 'closed won','mba membership completed','active retention save deal','active') then 1 else 0 end as is_enrolled
  , case when lower(pipeline_stage) IN ('current declines', 'closed won','active') then 1 else 0 end as is_active
  , case when lower(name_deal) LIKE "%in-person%" OR lower(name_deal) LIKE "%independent launch lab%" OR lower(name_deal) LIKE "%launch lab%" then 1 else 0 end as is_inperson


  , case when id_owner = 1426033370 AND (date(date_closed) > date('2024-08-23') AND date(date_closed) < date('2024-08-28')) then "Charlotte_8/24"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-08-16') AND date(date_closed) <= date('2024-08-22')) then "Raleigh_8/17"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-08-02') AND date(date_closed) <= date('2024-08-04')) and (name_product like "%Hybrid (PIF)%" OR
                                   name_product like "%6 Pay%" OR name_product like "%Virtual%" OR name_product like "%4 Pay%")
         then "Baltimore_8/3"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-07-26') AND date(date_closed) <= date('2024-07-28')) then "DC_7/28"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-07-19') AND date(date_closed) <= date('2024-07-21')) then "DC_7/20"
         when id_owner IN (980534468,1426033370) AND (date(date_closed) > date('2024-09-05') AND date(date_closed) <= date('2024-09-11') AND (name_product like "%Mastermind Business Academy%" OR name_deal like "%The Edge (PIF) - Chad Anderson%")) OR name_deal like "%The Edge (PIF) - Chris Robison%" then "Phoenix_9/6"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-09-19') AND date(date_closed) <= date('2024-09-22')) AND name_product like "%Mastermind Business Academy%" then "KBB LA + San Fran"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-10-09') AND date(date_closed) <= date('2024-10-13')) AND name_product like "%Mastermind Business Academy%" then "Workshop_10/10"
         when id_owner = 1426033370 AND (EXTRACT(year from date_closed) = 2024 AND EXTRACT(month from date_closed)= 4) AND name_product like "%Mastermind Business Academy%" then "Tampa"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-12-12') AND date(date_closed) <= date('2024-12-16')) AND name_product like "%Mastermind Business Academy%" then "BBW"
         when id_owner = 1426033370 AND FORMAT_DATE('%y%m', date_closed) = '2408' AND name_product like "%Mastermind Business Academy%" then "WS24"
         when id_owner = 1426033370 AND (date(date_closed) > date('2025-3-5') AND date(date_closed) <= date('2025-3-8')) AND name_product like "%Mastermind Business Academy%" then "KBB-3/25"
         else null end as workshop


  , case when id_owner in (746781400,597349111,285218005,746781399,1376975821,77523048,1957265727,597349111,1845545878,1211781353,980534468,1018935239,1018920904,626162146,455035854,626163393,1066123713,597349110,035145442,77523049,76677382,1332303546,78033661,1332303547,1211781353,77268027,143772806,455035854,1066123713,77268026,793323737,2027067907,76677381,78033660,358610520,2035145442) 
    OR
   (case when id_owner = 1426033370 AND (date(date_closed) > date('2024-08-23') AND date(date_closed) < date('2024-08-28')) then "Charlotte"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-08-16') AND date(date_closed) <= date('2024-08-22')) then "Raleigh"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-08-02') AND date(date_closed) <= date('2024-08-04')) and (name_product like "%Hybrid (PIF)%" OR
                                   name_product like "%6 Pay%" OR name_product like "%Virtual%" OR name_product like "%4 Pay%")
         then "Baltimore"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-07-26') AND date(date_closed) <= date('2024-07-28')) then "DC"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-07-19') AND date(date_closed) <= date('2024-07-21')) then "DC"
         when id_owner IN (980534468,1426033370) AND (date(date_closed) > date('2024-09-05') AND date(date_closed) <= date('2024-09-11') AND (name_product like "%Mastermind Business Academy%" OR name_deal like "%The Edge (PIF) - Chad Anderson%")) OR name_deal like "%The Edge (PIF) - Chris Robison%" then "Phoenix"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-09-19') AND date(date_closed) <= date('2024-09-22')) AND name_product like "%Mastermind Business Academy%" then "Workshop_9/20"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-10-09') AND date(date_closed) <= date('2024-10-13')) AND name_product like "%Mastermind Business Academy%" then "Workshop_10/10"
         when id_owner = 1426033370 AND (EXTRACT(year from date_closed) = 2024 AND EXTRACT(month from date_closed)= 4) AND name_product like "%Mastermind Business Academy%" then "Tampa"
         when id_owner = 1426033370 AND (date(date_closed) > date('2024-12-12') AND date(date_closed) <= date('2024-12-16')) AND name_product like "%Mastermind Business Academy%" then "BBW"
         when id_owner = 1426033370 AND (date(date_closed) > date('2025-3-5') AND date(date_closed) <= date('2025-3-8')) AND name_product like "%Mastermind Business Academy%" then "KBB-3/25"
         else null end) is not null 
    OR
   ((case when name_product LIKE "%The Action Academy%" then "TAA"  -- all
         when name_product LIKE "%Mastermind Business Academy%" then "MBA" -- all
         when name_product LIKE "%1:1 Coaching%" then "1:1" -- all
         --when name_product LIKE "%The Edge%" then "Edge" -- team only
         when name_product LIKE "%The Coaching Academy%" then "TCA" -- all
         --when name_product LIKE "%High Ticket Coaching%" then "HTC" --team
         --when name_product LIKE "%High Ticket Academy%" then "HTA" -- team
         when name_product LIKE "Independent Launch Lab Package" then "LL"
         else null end) is not null and id_owner != 1426033370)

    OR 
    ((case --when name_product LIKE "%The Action Academy%" then "TAA"  -- all
         --when name_product LIKE "%Mastermind Business Academy%" then "MBA" -- all
         --when name_product LIKE "%1:1 Coaching%" then "1:1" -- all
         when name_product LIKE "%The Edge%" then "Edge" -- team only
         --when name_product LIKE "%The Coaching Academy%" then "TCA" -- all
         when name_product LIKE "%High Ticket Coaching%" then "HTC" --team
         when name_product LIKE "%High Ticket Academy%" then "HTA" -- team
         when name_product LIKE "%Zenith%" THEN "Zenith"
         else null end) is not null and id_owner in (746781400,597349111,285218005,746781399,1376975821,77523048,1957265727,597349111,1845545878,1211781353,980534468,1018935239,1018920904,626162146,455035854,626163393,1066123713,597349110,035145442,77523049,76677382,1332303546,78033661,1332303547,1211781353,77268027,143772806,455035854,1066123713,77268026,793323737,2027067907,76677381,78033660,358610520,2035145442))



    OR
    ((case when id_owner = 1426033370 and (date(date_closed) > date('2024-8-4') and date(date_closed) < date('2024-8-2')) then 1 else null end) IS not null)






         then 1 else 0 end as team_sales


  ,   case when lower(name_deal) LIKE ('%hybrid%') then "Plus"
         when lower(name_deal) LIKE ('%plus%') then "Plus"
         when lower(name_deal) LIKE ('%virtual%') then "Virtual"
         when lower(name_deal) LIKE ('%affirm%') then "Affirm"
         when lower(name_deal) LIKE ('%lite%') then "Lite"
         when lower(name_deal) LIKE ('%in-person%') then "In-Person"
      --   when lower(name_deal) LIKE ('%plus%') then "Plus"
         else "Virtual" end as product_type
  , case when lower(id_price) like "%plus%" then 1 else 0 end as is_plus_plan
  , cast(min(coalesce(date_setter_assigned, date_owner_assigned)) as date) as date_assigned
  , CASE WHEN date_unpaused is null then date_diff(current_date, date(date_paused), day) 
    ELSE date_diff(date_unpaused, date_paused, day) END as days_paused
  , CASE WHEN date_unpaused = date_paused then 1 else 0 end as is_not_real_pause
from hubspot h
LEFT JOIN `bbg-platform.dbt_tscrivo.dim_email` e
  on h.email = e.email_all
WHERE (analytics.fnEmail_IsTest(email) = false or email = 'chrisj@remotestaff.com')
group by all