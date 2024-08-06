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
 -- WHERE c.status = "succeeded"
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
  , row_number() over(partition by cu.email,c.deal_id, i.subscription_id order by pi.created asc) as num_payment
  , c.statement_descriptor
  , c.metadata as metadata_charge
  , hsp.property_hs_folder_name
  , c.refunded
  , hsp.property_name
  , coalesce(cast(d2.deal_id as string), c.deal_id) as id_deal
  , date_refund
  , amount_refund
  , concat(cu.email,coalesce(p.id, hsp.property_product_id),coalesce(hsp.property_pricing_id, il.price_id)) as joinkey
  , right(coalesce(hsp.property_pricing_id, il.price_id),1) as plan_type
 -- , s.id as id_subscription
 , datetime(d.property_closedate,'America/Phoenix') as date_closed
  , d.property_product_name as name_product_plantype
  , count(*) as count
  , date(datetime(pi.created,'America/Phoenix')) as dt
  , pi.created
  , row_number() over(partition by email order by pi.created desc) as last_attempt
  , pr.id
  , c.deal_id
  , ps.label as pipeline_stage
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
LEFT JOIN `bbg-platform.hubspot2.deal` d
  on c.deal_id = cast(d.deal_id as string)
LEFT JOIN `bbg-platform.hubspot2.deal` d2
  on lower(cu.email) = lower(d2.property_email_address_of_contact)
  and pr.id = d2.property_pricing_id
LEFT JOIN `hubspot2.deal_pipeline_stage` ps
  on cast(d2.deal_pipeline_stage_id as string) = ps.stage_id
WHERE true
  and analytics.fnEmail_IsTest(email) = false
--  and s.funnel_id = '13216474'
--  and pi.status = "succeeded"
 -- and pr.id IN ('MBA_pif_9497_1', 'MBA_pp_1497_7')
--  and email = "gmc@holisten.no"
--  and c.refunded = false
-- and coalesce(p.name, c.statement_descriptor, pi.description) LIKE "%Business Academy%"
 group by all
ORDER BY pi.created desc