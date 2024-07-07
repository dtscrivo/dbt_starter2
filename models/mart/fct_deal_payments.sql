{{ config(materialized='table') }}


  with charge as (
SELECT c.payment_intent_id
  , TIMESTAMP_SUB(c.created, INTERVAL 7 HOUR) as createdd
  , c.status
  , row_number() over (partition by c.payment_intent_id order by c.created asc) as charge_num
  , row_number() over (partition by c.payment_intent_id order by c.created desc) as charge_success_num
  , c.id
  , c.outcome_reason
  , json_extract_scalar(c.metadata, "$.processor") as processor
  , statement_descriptor
  , c.metadata
  , json_extract_scalar(c.metadata, "$.product_id") as object_id
  , refunded
  , coalesce(cast(m.deal_id as string), json_extract_scalar(c.metadata, "$.deal_id")) as deal_id
  , r.created as date_refund
  , r.amount/100 as amount_refund
  FROM `bbg-platform.stripe_mindmint.charge` c
  LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
    on cast(json_extract_scalar(c.metadata, "$.deal_id") as string) = cast(m.merged_deal_id as string)
  LEFT JOIN `bbg-platform.stripe_mindmint.refund` r
    on c.id = r.charge_id
  WHERE c.status = "succeeded"
--WHERE payment_intent_id = "pi_3Ma1jwISjDEJDDVR15x1pyVV"
qualify row_number() over (partition by c.payment_intent_id order by c.created desc) = 1
)

, subs AS(
SELECT s.id
  , TIMESTAMP_SUB(s.created, INTERVAL 7 HOUR) as date_sub_created
  , s.status
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as funnel_id
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as funnel_name
FROM `bbg-platform.stripe_mindmint.subscription_history` s
where cast(_fivetran_end as string) LIKE "9999%"
--qualify row_number() over(partition by s.id order by created desc) = 1
)

SELECT  pi.id as id_payment_intent
  , pi.amount/100 as amount_pi
  , pi.amount_received/100 as amount_collected
  , pi.customer_id as id_customer
  , pi.description
  , pi.payment_method_id as id_payment_method_id
  , pi.status as status_payment_intent
  , TIMESTAMP_SUB(pi.created, INTERVAL 7 HOUR) as date_pi_created
  , c.charge_num
  , c.charge_success_num
  , c.status as status_charge
  , i.status as status_invoice
  , case when c.status = "succeeded" and charge_num = 1 then 'success' else 'fail' end as first_payment
  , case when c.status IS NULL then "no-charge"
    when c.status = "succeeded" and charge_num = 1 then 'first_charge_success'
    when c.status = "succeeded" and charge_num != 1 then 'fail_recovered'
    else "fail_not_recovered" 
     end as category
  , i.subscription_id as id_subscription
  , cu.email
  , cu.name
  , i.id as id_invoice
  , c.createdd as date_charge
  , c.id as id_charge
  , hosted_invoice_url
  , invoice_pdf
  , coalesce(hsp.property_pricing_id, il.price_id) as id_price
  , coalesce(pr.unit_amount/100, hsp.property_price) as price
  , coalesce(p.name, c.statement_descriptor, pi.description) as name_product
  , coalesce(p.id, hsp.property_product_id) as id_product
  , s.status as status_subscription
  , s.date_sub_created
  , c.outcome_reason
  , pr.recurring_interval
  , s.funnel_id as id_funnel
  , s.funnel_name as name_funnel
  , case when c.processor IS NULL AND s.funnel_id IS NULL THEN "payment_gateway"
       when c.processor IS NULL AND s.funnel_id IS NOT NULL THEN "click_funnel"
       else c.processor end as processor
  , row_number() over(partition by cu.email,deal_id, i.subscription_id order by pi.created asc) as num_payment
  , c.statement_descriptor
  , c.metadata as metadata_charge
  , hsp.property_hs_folder_name
  , c.refunded
  , hsp.property_name
  , deal_id as id_deal
  , date_refund
  , amount_refund
  , concat(cu.email,coalesce(p.id, hsp.property_product_id),coalesce(hsp.property_pricing_id, il.price_id)) as joinkey
  , right(coalesce(hsp.property_pricing_id, il.price_id),1) as plan_type
 -- , s.id as id_subscription
FROM `bbg-platform.stripe_mindmint.payment_intent` pi
LEFT JOIN charge c
  on pi.id = c.payment_intent_id
LEFT JOIN `bbg-platform.stripe_mindmint.invoice` i 
  on pi.id = i.payment_intent_id
LEFT JOIN `bbg-platform.stripe_mindmint.invoice_line_item` il
  on i.id = il.invoice_id
LEFT JOIN `bbg-platform.stripe_mindmint.price` pr
  on il.price_id = pr.id
LEFT JOIN `bbg-platform.stripe_mindmint.product` p
  on pr.product_id = p.id
LEFT JOIN `bbg-platform.stripe_mindmint.customer` cu
  on pi.customer_id = cu.id
LEFT JOIN subs s
  on i.subscription_id = s.id
LEFT JOIN `bbg-platform.hubspot2.product` hsp
  on c.object_id = cast(hsp.property_hs_object_id as string)
WHERE true
  and pi.status = "succeeded"
 -- and email = "sab2611@hotmail.com"
--  and c.refunded = false
-- and coalesce(p.name, c.statement_descriptor, pi.description) LIKE "%Business Academy%"
ORDER BY pi.created desc


'''
WITH base as (
  WITH deal_source AS (
Select d.deal_id as id_deal
   , property_product_name as name_product_plantype
   , property_product_status as status_deal
   , property_dealname as name_deal
   , DATE(d.property_createdate) as date_created
   , property_email_address_of_contact as customer_email
   , is_deleted as is_deleted
   , concat(property_first_name_of_contact_record, " ", property_last_name_of_contact_record) as name_property
   , case when contains_substr(property_product_name, "PIF") then "PIF" ELSE "PP" END as pay_type
   , property_wire_payment_ as is_wire
   , property_subscription_id as id_stripe_sub
   , property_transaction_id as id_stripe_paymentintent
   , property_hs_merged_object_ids as merged_deals
   , property_closedate as date_closed
   , p.property_product_id as id_product
   , property_hs_acv as amount_contract
   , property_future_contracted_value as amount_owed
   , case when property_product_name = "1 × The Action Academy (at $1,637.00 / month)" then "taa_pp_nolp_1637_4"
          when property_product_name = "1 × The Action Academy (at $5,997.00 / month)" then "taa_pif_nolp_5997_1"
          when property_product_name = "The Action Academy (PIF)" then "taa_pif_nolp_5997_1"
          when property_product_name = "The Action Academy ($935.00*7)" then "taa_pp_lp_935_7"
          when property_product_name = "The Action Academy ($1637.00*4)" then "taa_pp_nolp_1637_4"
          when property_product_name = "The Action Academy (Launchpad Not Included) (4 Pay)" then "taa_pp_nolp_1637_4"
          when property_product_name = "The Action Academy TCA Dropout Or Transfer Package (PIF)" then "taa_pif_nolp_5997_1"
          when property_product_name = "The Action Academy (Launchpad Not Included) (PIF)" then "taa_pif_nolp_5997_1"
             when property_product_name = "Mastermind Business Academy: In-Person Package" then "MBA_pif_inpersonpackage_5997_1"
          else p.property_pricing_id end as pricing_id
   , p.property_pricing_id
   , property_amount
FROM `bbg-platform.hubspot2.deal` d
LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
   on d.deal_id = m.merged_deal_id
LEFT JOIN `bbg-platform.hubspot2.product` p
   on d.property_product_name = p.property_name
WHERE m.merged_deal_id IS NULL
   and is_deleted = FALSE
qualify row_number() over (partition by property_email_address_of_contact,property_dealname order by property_closedate desc) = 1
)

, wires as (
    SELECT deal_id
  , sum(property_amount) as property_amount
  , property_product_name
  , sum(property_amount_collected)
  , sum(property_amount_in_home_currency)
FROM `bbg-platform.hubspot2.deal`
WHERE true
  and property_wire_payment_ = TRUE
  group by all
)

, payments as (
  SELECT coalesce(cast(m.deal_id as string), json_extract_scalar(c.metadata, "$.deal_id")) as deal_id
    , row_number() over( partition by coalesce(cast(m.deal_id as string), json_extract_scalar(c.metadata, "$.deal_id")), c.status order by c.created asc) as num_payments
    , row_number() over( partition by coalesce(cast(m.deal_id as string), json_extract_scalar(c.metadata, "$.deal_id")), c.status order by c.created desc) as last_payment
    , c.metadata
    , c.id
    , c.created
    , c.amount/100 as amount_collected
    , c.status
    , c.refunded
    , r.created as date_refund
    , r.amount/100 as amount_refund
    , c.customer_id
    , c.payment_intent_id
    , c.invoice_id
  , json_extract_scalar(c.metadata, "$.processor") as processor
  FROM `bbg-platform.stripe_mindmint.charge` c
  LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
    on cast(json_extract_scalar(c.metadata, "$.deal_id") as string) = cast(m.merged_deal_id as string)
  LEFT JOIN `bbg-platform.stripe_mindmint.refund` r
    on c.id = r.charge_id
  WHERE c.status = "succeeded"
    and json_extract_scalar(c.metadata, "$.product_id") != "1727719140"
)


, clickfunnel AS(
SELECT json_extract_scalar(h.metadata, "$.netsuite_CF_funnel_id") as funnel_id
 , c.metadata
 , c.email
 , h.created
 , h.id
 , json_extract_scalar(c.metadata, "$.processor") as processor
FROM `bbg-platform.stripe_mindmint.subscription_history` h
LEFT JOIN `stripe_mindmint.customer` c
  on h.customer_id = c.id
WHERE json_extract_scalar(h.metadata, "$.netsuite_CF_funnel_id") = "13216474"
  and json_extract_scalar(c.metadata, "$.deal_id") = "11383028672"
qualify row_number() over( partition by c.email order by h.created desc ) = 1
)



, sub_invoice AS (
SELECT subscription_id
  , sum(amount_paid / 100) as amount_paid
  , row_number() over(partition by subscription_id, status order by created asc) as payment_num
  , created
  , status
  , amount_paid / 100 as amount
FROM `bbg-platform.stripe_mindmint.invoice` 
where true 
   and status = "paid"
GROUP BY ALL
QUALIFY row_number() over(partition by subscription_id, status order by created desc) = 1
)

, forms AS (
SELECT deal_id
  , f.title
  , case when f.title = "Attendance - Mastermind Preview Event" then 1 else 0 end as preview_event
  , case when f.title = "Attendance - 2024 Knowledge Business Bootcamp" then 1 else 0 end as kbb_optin
  , case when f.title = "Attendance - Mastermind Preview Event 3.27.24" then 1 else 0 end as preview_event_327
  , case when f.title = "Attendance - Mastermind Preview Event In-Person" then 1 else 0 end as preview_event_inperson
FROM `bbg-platform.hubspot2.deal_contact` dc
LEFT JOIN `bbg-platform.hubspot2.contact` c
  on dc.contact_id = c.id
LEFT JOIN `hubspot2.contact_form_submission` f
  on c.id = f.contact_id
where title IN ('Attendance - Mastermind Preview Event','Attendance - 2024 Knowledge Business Bootcamp','Attendance - Mastermind Preview Event 3.27.24','Attendance - Mastermind Preview Event In-Person')
GROUP BY deal_id, f.title
)

SELECT d.*
   , pp.amount_refund
  , pp.id as id_charge
  , pp.customer_id as id_customer
  , pp.payment_intent_id as id_payment_intent
  , pp.invoice_id as id_invoice
  , right(d.pricing_id, 1) as num_plan_payments
  , cf.funnel_id
  , case when pp.processor = "inperson" then "inperson"
         when cf.funnel_id = "13216474" then "clickfunnel"
         when cf.funnel_id IS NOT NULL then "clickfunnel"
         when cf.processor = "gateway" then "gateway"
         else "gateway" end as payment_source
  , cf.id as id_subscription

  , coalesce(pp.amount_collected, si.amount_paid) as amount_collected
  , coalesce(pp.num_payments, si.payment_num) as num_payments_made
  , coalesce(pp.created, si.created) as date_charge
  , pp.date_refund
  , pp.refunded
  , w.property_amount as wire
FROM  deal_source d
LEFT JOIN payments pp
  ON cast(d.id_deal as string) = cast(pp.deal_id as string)
LEFT JOIN clickfunnel cf
  on d.customer_email = cf.email
LEFT JOIN sub_invoice si
  on cf.id = si.subscription_id
LEFT JOIN wires w
  on d.id_deal = w.deal_id
WHERE d.customer_email NOT LIKE '%graziosi.com' AND d.customer_email NOT LIKE '%@mastermind.com'
 -- and d.id_deal = 9560860865

   and d.is_deleted = FALSE
   and d.customer_email NOT LIKE "%@deangraziosi.com"
   and d.customer_email NOT LIKE "%@mastermind.com"
   and d.customer_email NOT LIKE "%armartin%"
   and d.customer_email NOT LIKE "%mailinator%"

)

SELECT customer_email
  , id_charge
  , id_customer
  , id_payment_intent
  , id_invoice
   ,  amount_collected
   , num_payments_made
   , num_plan_payments
   , amount_contract
   , FLOOR(DATE_DIFF(CURRENT_DATE(), DATE(date_closed), MONTH)) as age_mob
   , FLOOR(DATE_DIFF(DATE(date_charge), DATE(date_closed), MONTH)) as charge_mob
   , wire
   , case when wire is not null then 1 else 0 end as is_wire
   , CEIL(DATE_DIFF(DATE(date_refund), DATE(date_closed), DAY) / 7) as refund_mob
   , id_deal
   , date_closed
   , date_charge
   , case when amount_refund IS NOT NULL then 1 else 0 end as refunded
   , date_refund
   , amount_refund
   , pricing_id
   , name_product_plantype
  , DATE_ADD(cast(date_charge as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_charge) + 8, 7) DAY) AS charge_week
  , DATE_ADD(cast(date_refund as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_refund) + 8, 7) DAY) AS refund_week
  , DATE_ADD(cast(date_closed as date), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM date_closed) + 8, 7) DAY) AS order_week
  , CEIL(date_diff(date(date_charge), DATE(date_closed), day) / 7) as charge_wob
  , CEIL(date_diff(date(date_refund), DATE(date_closed), day) / 7) as refund_wob
  , CEIL(date_diff(current_date, DATE(date_closed), day) / 7) as order_wob
  , case when payment_source = "clickfunnel" and date(date_closed) < DATE('2024-04-01') then 1 else 0 end as KBB
  , payment_source
  , case when name_product_plantype LIKE "%The Action Academy%" then "TAA" 
         when name_product_plantype LIKE "%Mastermind Business Academy%" then "MBA"
         else "" end as program
  , case when amount_refund IS NULL then amount_collected else (amount_collected - amount_refund) end as net
  , case when amount_collected IS NOT NULL and (amount_collected - amount_refund) = 0 then 1 else 0 end as full_refund
FROM Base
WHERE true
  and date_charge IS NOT NULL
--  and customer_email = "katrmccusker@gmail.com"
ORDER BY id_deal desc

'''