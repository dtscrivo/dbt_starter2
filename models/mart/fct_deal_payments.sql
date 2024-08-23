{{ config(materialized='table') }}

  

with base as (
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
  , c.amount/100 as amount_charge
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
  , case when c.status = "succeeded" and charge_num = 1 then 'success' else 'fail' end as first_charge_attempt
  , case when c.status IS NULL then "no-charge"
    when c.status = "succeeded" and charge_num = 1 then 'first_charge_success'
    when c.status = "succeeded" and charge_num != 1 then 'fail_recovered'
    else "fail_not_recovered" 
     end as category_charge
  , i.subscription_id as id_subscription
  , analytics.fnEmail(cu.email) as email
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
  , dense_rank() over(partition by cu.email,coalesce(hsp.property_pricing_id, il.price_id) order by pi.created asc) as num_payment
  , c.statement_descriptor
  , c.metadata as metadata_charge
  , hsp.property_hs_folder_name as name_product_group
  , c.refunded
  , hsp.property_name
  , coalesce(c.deal_id, cast(d2.deal_id as string)) as deal_id
  , date_refund
  , amount_refund
  , amount_charge
  , amount_charge-amount_refund as amount_net
  , concat(cu.email,coalesce(p.id, hsp.property_product_id),coalesce(hsp.property_pricing_id, il.price_id)) as joinkey
  , right(coalesce(hsp.property_pricing_id, il.price_id),1) as plan_type
 -- , s.id as id_subscription
 , datetime(d.property_closedate,'America/Phoenix') as date_closed
  , d.property_product_name as name_product_plantype
  , concat(o.first_name," ",o.last_name) as name_owner
  , d.property_setter_name as name_setter
  , TIMESTAMP_SUB(i.created, INTERVAL 7 HOUR) as date_invoice
  , coalesce(amount_charge-amount_refund, amount_charge) as net
  -- , case when payment_source = "clickfunnel" and date(date_closed) < DATE('2024-04-01') then "KBB" 
  --     when id_funnel = '13216474' and FORMAT_DATE('%y%m', date_closed) = '2408' then "ws24"
  --     else null end as event
  , hsp2.id as id_hs_object
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
LEFT JOIN `bbg-platform.hubspot2.product` hsp2
  on p.id = cast(hsp.property_product_id as string)
LEFT JOIN `bbg-platform.hubspot2.deal` d
  on c.deal_id = cast(d.deal_id as string)
LEFT JOIN `dbt_tscrivo.dim_email` e
  on cu.email = e.email_prime
LEFT JOIN `bbg-platform.hubspot2.deal` d2
  on concat(email_all,coalesce(hsp.property_pricing_id, il.price_id)) = concat(analytics.fnEmail(d2.property_email_address_of_contact),d2.property_pricing_id)
  and d2.deal_id is not null
LEFT JOIN `bbg-platform.hubspot2.owner` o
  ON d.owner_id= o.owner_id
WHERE true
  and pi.status = "succeeded"
 -- and cu.email = "cheryl@the4seasons.pro"
--  and c.refunded = false
-- and coalesce(p.name, c.statement_descriptor, pi.description) LIKE "%Business Academy%"

qualify row_number() over(partition by id_payment_intent order by case when deal_id is null then 1 else 0 end asc) = 1



)

--, every as (
  select b.*
    , coalesce(cast(m.deal_id as string), b.deal_id,b2.deal_id, cast(d.deal_id as string), cast(d2.deal_id as string)) as id_deal
    , dense_rank() over(partition by b.email, b.id_product order by b.date_pi_created desc) as recency
    , dense_rank() over(partition by b.email, b.id_product order by b.date_pi_created asc) as num_payments
    , coalesce(e.email_prime, b.email) as email_prime
  from base b
  left join base b2
    on concat(b.email,b.id_product) = concat(b2.email,b2.id_product)
  LEFT JOIN `dbt_tscrivo.dim_email` e
    on b.email = e.email_all
  left join `hubspot2.deal` d
    on concat(b.email,b.id_product) = concat(analytics.fnEmail(d.property_email_address_of_contact),d.property_product_id)
    left join `hubspot2.deal` d2
    on concat(e.email_prime,b.id_product) = concat(analytics.fnEmail(d2.property_email_address_of_contact),d2.property_product_id)
  LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
    on coalesce(b.deal_id,b2.deal_id, cast(d.deal_id as string)) = cast(m.merged_deal_id as string)

  WHERE analytics.fnEmail_IsTest(b.email) = false
  --  and (b.name_product LIKE "%Business Academy" or b.name_product LIKE "%Action Academy%")
 --   and b.email = 'eastgreetswestegg@gmail.com'
--    and  coalesce(cast(m.deal_id as string), b.deal_id,b2.deal_id, cast(d.deal_id as string)) = '12080028362'
  qualify row_number() over(partition by b.id_payment_intent, date_pi_created order by case when email is not null then 1 else 0 end desc) = 1