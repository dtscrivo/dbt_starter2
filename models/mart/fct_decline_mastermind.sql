--after 4/01/2024

{{ config(materialized='table') }}

with charges as (
with charge as (
SELECT payment_intent_id
  , TIMESTAMP_SUB(created, INTERVAL 7 HOUR) as createdd
  , status
  , row_number() over (partition by payment_intent_id order by created asc) as charge_num
  , row_number() over (partition by payment_intent_id order by created desc) as charge_success_num
  , id
  , outcome_reason
FROM `bbg-platform.stripe_mastermind.charge`
--WHERE payment_intent_id = "pi_3Ma1jwISjDEJDDVR15x1pyVV"
qualify row_number() over (partition by payment_intent_id order by created desc) = 1
)

, subs AS(
SELECT s.id
  , TIMESTAMP_SUB(s.created, INTERVAL 7 HOUR) as date_sub_created
  , s.status
FROM `bbg-platform.stripe_mastermind.subscription_history` s
where cast(_fivetran_end as string) LIKE "9999%"
--qualify row_number() over(partition by s.id order by created desc) = 1
)

SELECT  pi.id as id_payment_intent
  , pi.amount/100 as pi_amount
  , pi.amount_received/100 as pi_amount_received
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
  , il.price_id as price_id
  , pr.unit_amount/100 as price
  , p.name as product
  , p.id as id_product
  , s.status as status_subscription
  , s.date_sub_created
  , c.outcome_reason
FROM `bbg-platform.stripe_mastermind.payment_intent` pi
LEFT JOIN charge c
  on pi.id = c.payment_intent_id
LEFT JOIN `bbg-platform.stripe_mastermind.invoice` i 
  on pi.id = i.payment_intent_id
LEFT JOIN `bbg-platform.stripe_mastermind.invoice_line_item` il
  on i.id = il.invoice_id
LEFT JOIN `bbg-platform.stripe_mastermind.price` pr
  on il.price_id = pr.id
LEFT JOIN `bbg-platform.stripe_mastermind.product` p
  on pr.product_id = p.id
LEFT JOIN `bbg-platform.stripe_mastermind.customer` cu
  on pi.customer_id = cu.id
LEFT JOIN subs s
  on i.subscription_id = s.id

WHERE true
  --and pi.id = "pi_3LHEs1ISjDEJDDVR0rMUQ6F5"
  --and i.subscription_id = "sub_1PLAEwISjDEJDDVRS967ZTnI"
  --and i.id = "in_1P5XxIISjDEJDDVRa8ng8gDp"
 -- and cu.email = "seanamaura3@gmail.com"
   --and pr.unit_amount/100 = 997
ORDER BY pi.created desc
 -- and i.subscription_id IS NOT NULL
 -- and pi.description != "Subscription creation"
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
   , c.property_email as ticket_email
FROM `bbg-platform.hubspot2.ticket` t
LEFT JOIN `bbg-platform.hubspot2.ticket_deal` d
  on t.id = d.ticket_id
LEFT JOIN `hubspot2.ticket_contact` tc
  on t.id = tc.ticket_id
LEFT JOIN `hubspot2.contact` c
  on tc.contact_id = c.id
WHERE true
  and t.property_hs_pipeline = 17148918 -- gravy pipeline
--  and t.property_subject LIKE "%Yearly%"
qualify row_number() over (partition by d.deal_id, t.property_hs_pipeline order by t.property_createdate desc) = 1
)


, chargenum as (
SELECT payment_intent_id
  , TIMESTAMP_SUB(created, INTERVAL 7 HOUR) as createdd
  , status
  , row_number() over (partition by payment_intent_id order by created asc) as charge_num
  , row_number() over (partition by payment_intent_id order by created desc) as charge_success_num
  , id
  , failure_message
  , failure_code

FROM `bbg-platform.stripe_mastermind.charge`
WHERE true
  --and payment_intent_id = "pi_3Ma1jwISjDEJDDVR15x1pyVV"
  and status IS NOT null

)

  SELECT c.* 
  , date_diff(c.date_charge, c.date_pi_created, day) as days_to_charge
  , case when date(c.date_pi_created) < date('2024-04-01') then 1 else 0 end as gravy
  , case when c.category = "first_charge_success" then 0 else 1 end as first_charge_fail
  , case when c.category = 'fail_recovered' then 1 else 0 end as recovered
  , case when date(c.date_pi_created) < date('2024-04-01') then 0 else 1 end as inhouse
  , concat(extract(month from c.date_pi_created),"-",extract(year from c.date_pi_created)) as pi_date
  , date_diff(current_date, date(c.date_pi_created), day) as dob
  , case when c.product LIKE "%Monthly%" then 1 else 0 end as monthly
  , case when date(c.date_pi_created) >= date('2023-11-23') and date(c.date_pi_created) <= date('2023-11-30') then 1 else 0 end as blackfriday
  , case when date(c.date_pi_created) >= date('2024-01-05') and date(c.date_pi_created) <= date('2024-01-11') then 1 else 0 end as Jan
  , case when id_ticket is not null then 1 else 0 end as ticket
  , date_diff(t.date_ticket_created, c.date_pi_created,day) as days_to_ticket
  , date_diff(c.date_charge, c.date_pi_created,day) as first_charge_days
  , date_diff(cn2.createdd, cn.createdd, day) as first_retry_day
  , cn.failure_message
  , cn.failure_code
FROM charges c
LEFT JOIN tickets t
  on lower(c.email) = lower(t.ticket_email)
LEFT JOIN chargenum cn
  on c.id_payment_intent = cn.payment_intent_id
  and cn.charge_num = 1
LEFT JOIN chargenum cn2
  on c.id_payment_intent = cn2.payment_intent_id
  and cn2.charge_num = 2
--WHERE date(c.date_pi_created) > date('2023-04-01')

