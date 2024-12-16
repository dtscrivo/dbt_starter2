
{{ config(materialized='table') }}


with dim_email as (
WITH ids_to_exclude AS (
  SELECT DISTINCT CAST(id AS INT64) AS id
  FROM `bbg-platform.hubspot2.contact`,
  UNNEST(SPLIT(REPLACE(property_hs_merged_object_ids, ';', ','), ',')) AS id
  WHERE SAFE_CAST(id AS INT64) IS NOT NULL
)


, base as (
WITH emails AS (
  WITH example_data AS (
    SELECT property_hs_additional_emails AS email_string,
           analytics.fnEmail(property_email) as property_email
    FROM `hubspot2.contact`
    WHERE true
  AND is_deleted = FALSE
  and property_email is not null and property_email != ""
  AND id NOT IN (
    SELECT id 
    FROM ids_to_exclude
  )
  
  )
  SELECT
    property_email,
    SPLIT(email_string, ";")[SAFE_OFFSET(0)] AS email_1,
    SPLIT(email_string, ";")[SAFE_OFFSET(1)] AS email_2,
    SPLIT(email_string, ";")[SAFE_OFFSET(2)] AS email_3,
    SPLIT(email_string, ";")[SAFE_OFFSET(3)] AS email_4,
    SPLIT(email_string, ";")[SAFE_OFFSET(4)] AS email_5,
    SPLIT(email_string, ";")[SAFE_OFFSET(5)] AS email_6,
    SPLIT(email_string, ";")[SAFE_OFFSET(6)] AS email_7,
    SPLIT(email_string, ";")[SAFE_OFFSET(7)] AS email_8,
    SPLIT(email_string, ";")[SAFE_OFFSET(8)] AS email_9
  FROM example_data
)

SELECT property_email as email_prime, property_email AS email_all FROM emails
UNION ALL
SELECT property_email as email_prime, email_1 AS email_all FROM emails WHERE email_1 IS NOT NULL
UNION ALL
SELECT property_email, email_2 AS email FROM emails WHERE email_2 IS NOT NULL
UNION ALL
SELECT property_email, email_3 AS email FROM emails WHERE email_3 IS NOT NULL
UNION ALL
SELECT property_email, email_4 AS email FROM emails WHERE email_4 IS NOT NULL
UNION ALL
SELECT property_email, email_5 AS email FROM emails WHERE email_5 IS NOT NULL
UNION ALL
SELECT property_email, email_6 AS email FROM emails WHERE email_6 IS NOT NULL
UNION ALL
SELECT property_email, email_7 AS email FROM emails WHERE email_7 IS NOT NULL
UNION ALL
SELECT property_email, email_8 AS email FROM emails WHERE email_8 IS NOT NULL
UNION ALL
SELECT property_email, email_9 AS email FROM emails WHERE email_9 IS NOT NULL
)

Select *
FROM base
qualify row_number() over(partition by email_prime, email_all) = 1
)

, deals as (
  select d.*,
  e.email_prime as email
  -- analytics.fnEmail(property_email_address_of_contact) as email
 -- , coalesce(d.property_product_id, p.property_product_id) as id_product
  , coalesce(p.property_product_id, d.property_product_id) as id_product
from `hubspot2.deal` d
left join `hubspot2.product` p 
  on d.property_pricing_id = p.property_pricing_id

left join dim_email e
  on analytics.fnEmail(property_email_address_of_contact) = email_all
   -- where deal_id = 10367994105
) 
 
 
 
 , base as (
     WITH mindmint_charge AS (
  SELECT 
    c.payment_intent_id,
    c.status,
    c.id,
    JSON_EXTRACT_SCALAR(c.metadata, "$.product_id") AS object_id,
    JSON_EXTRACT_SCALAR(c.metadata, "$.rep_id") AS rep_id,
    JSON_EXTRACT_SCALAR(c.metadata, "$.deal_id") AS deal_id
  FROM `bbg-platform.stripe_mindmint.charge` c
 -- where c.id = 'ch_3Q5y62LYbD2uWeLi0CnTVqv3'
  qualify row_number() over(partition by c.payment_intent_id order by c.created desc) = 1
)

  , mastermind_charge AS (
  SELECT 
    c.payment_intent_id,
    c.status,
    c.id,
    JSON_EXTRACT_SCALAR(c.metadata, "$.product_id") AS object_id,
    JSON_EXTRACT_SCALAR(c.metadata, "$.rep_id") AS rep_id,
    JSON_EXTRACT_SCALAR(c.metadata, "$.deal_id") AS deal_id
  FROM `bbg-platform.stripe_mastermind.charge` c
  qualify row_number() over(partition by c.payment_intent_id order by c.created desc) = 1
)


, mindmint_subs as (
  SELECT s.id
  , DATETIME(s.created, 'America/Phoenix') as date_sub_created
  , s.status
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as funnel_id
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as funnel_name
FROM `bbg-platform.stripe_mindmint.subscription_history` s
where cast(_fivetran_end as string) LIKE "9999%"
)

, mastermind_subs as (
  SELECT s.id
  , DATETIME(s.created, 'America/Phoenix') as date_sub_created
  , s.status
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as funnel_id
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as funnel_name
FROM `bbg-platform.stripe_mastermind.subscription_history` s
where cast(_fivetran_end as string) LIKE "9999%"
)







, mastermind_emails as (
  select id as id_customer
   , CASE
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.(con|comm|cpm|coms|cin|come|xon|ccom|comu|vom|comio|coom|col|vcom|comin|cim|clm|comt|comc|coma|comp|ckm|comcom|xom|comd|comuc)$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.(con|comm|cpm|coms|cin|come|xon|ccom|comu|vom|comio|coom|col|vcom|comin|cim|clm|comt|comc|coma|comp|ckm|comcom|xom|comd|comuc)$', '.com')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.orgg$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.orgg$', '.org')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.(ner|nyt|nwt|ney)$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.(ner|nyt|nwt|ney)$', '.net')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.(co.um|co.uj|co.un)$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.(co.um|co.uj|co.un)$', '.co.uk')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\@hot.ailcom$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\@hot.ailcom$', '@hotmail.com')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.dee$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.dee$', '.de')
    ELSE 
      analytics.fnEmail(cu.email) end as email
  from `bbg-platform.stripe_mastermind.customer` cu
)

, mindmint_emails as (
  select id as id_customer
   , CASE
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.(con|comm|cpm|coms|cin|come|xon|ccom|comu|vom|comio|coom|col|vcom|comin|cim|clm|comt|comc|coma|comp|ckm|comcom|xom|comd|comuc)$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.(con|comm|cpm|coms|cin|come|xon|ccom|comu|vom|comio|coom|col|vcom|comin|cim|clm|comt|comc|coma|comp|ckm|comcom|xom|comd|comuc)$', '.com')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.orgg$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.orgg$', '.org')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.(ner|nyt|nwt|ney)$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.(ner|nyt|nwt|ney)$', '.net')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.(co.um|co.uj|co.un)$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.(co.um|co.uj|co.un)$', '.co.uk')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\@hot.ailcom$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\@hot.ailcom$', '@hotmail.com')
    WHEN REGEXP_CONTAINS(analytics.fnEmail(cu.email), r'\.dee$') THEN 
      REGEXP_REPLACE(analytics.fnEmail(cu.email), r'\.dee$', '.de')
    ELSE 
      analytics.fnEmail(cu.email) end as email
  from `bbg-platform.stripe_mindmint.customer` cu
)

-- , mastermind AS (
  SELECT
    pi.id AS id_payment_intent,
    pi.amount/100 as amount_pi,
    pi.customer_id AS id_customer,
    pi.status AS status_payment_intent,
    DATETIME(pi.created, 'America/Phoenix') AS date_pi_created,
    c.status AS status_charge,
    i.status AS status_invoice,
    i.subscription_id AS id_subscription_invoice,
    cu.email
    , e.email_prime
    , i.id AS id_invoice,

    c.id AS id_charge,
    il.price_id AS id_price,
    pr.unit_amount / 100 AS price,
    p.name AS name_product,
    case when p.id = 'prod_OlvMv5no5FcGGX' then 'prod_O3DEZjh4Ib4yjK' else p.id end AS id_product,
    s.status AS status_subscription,

    s.id as id_subscription
  , "MM" as stripe_account
  , DATETIME(d.property_closedate, 'America/Phoenix') as date_closed
  , coalesce(cast(d.deal_id as string),cast(d6.deal_id as string), cast(d2.deal_id as string)) as id_deal

  , coalesce(c.rep_id, cast(d.owner_id as string)) as id_owner
  , s.funnel_id
  , s.funnel_name
  FROM `bbg-platform.stripe_mastermind.payment_intent` pi
  LEFT JOIN mastermind_charge c ON pi.id = c.payment_intent_id
  LEFT JOIN `bbg-platform.stripe_mastermind.invoice` i ON pi.id = i.payment_intent_id
  LEFT JOIN `bbg-platform.stripe_mastermind.invoice_line_item` il ON i.id = il.invoice_id
  LEFT JOIN `bbg-platform.stripe_mastermind.price` pr ON il.price_id = pr.id
  LEFT JOIN `bbg-platform.stripe_mastermind.product` p ON pr.product_id = p.id
  LEFT JOIN mastermind_emails cu ON pi.customer_id = cu.id_customer
  LEFT JOIN dim_email e
    on cu.email = e.email_all
  LEFT JOIN mastermind_subs s ON i.subscription_id = s.id
  LEFT JOIN `hubspot2.product` hp
    on p.id = cast(hp.property_product_id as string)
  LEFT JOIN `hubspot2.deal` d
    on c.deal_id = cast(d.deal_id as string)
LEFT JOIN `bbg-platform.hubspot2.deal_pipeline_stage` ps
  ON cast(d.deal_pipeline_stage_id as string) = ps.stage_id
LEFT JOIN `hubspot2.deal` d6
    on i.subscription_id = d6.property_subscription_id 
LEFT JOIN deals d2
  on concat(e.email_prime ,case when p.id = 'prod_OlvMv5no5FcGGX' then 'prod_O3DEZjh4Ib4yjK' else p.id end) = concat(d2.email, d2.id_product)
-- left join `dbt_tscrivo.dim_email` e
--       on email = e.email_all

  WHERE TRUE
 --   and analytics.fnEmail_IsTest(cu.email) = false

UNION ALL


-- mindmint AS (

  SELECT
    pi.id AS id_payment_intent,
    pi.amount/100 as amount_pi,
    pi.customer_id AS id_customer,
    pi.status AS status_payment_intent,
    DATETIME(pi.created, 'America/Phoenix') AS date_pi_created,
    c.status AS status_charge,
    case when i.status is null then "no invoice" else i.status end AS status_invoice,
    case when i.subscription_id is null then "no sub" else i.subscription_id end AS id_subscription_invoice,
    cu.email
    , e.email_prime
    , case when i.id is null then "no invoice" else i.id end AS id_invoice,
    c.id AS id_charge,

    coalesce(p.property_pricing_id, il.price_id) AS id_price,
    coalesce(p.property_price,pr.unit_amount/100) AS price,
    coalesce(p.property_name,pro.name) AS name_product,
    coalesce(p.property_product_id,pro.id) AS id_product,
    case when s.status is null then "no sub" else s.status end AS status_subscription,
    -- date('9999-12-31') as date_sub_created,
    case when s.id is null then "no sub" else s.id end as id_subscription
  , "BBG" as stripe_account
  , coalesce(DATE(d.property_closedate),DATE(d2.property_closedate)) as date_closed
   , coalesce(cast(d4.deal_id as string), cast(d.deal_id as string),cast(d6.deal_id as string), cast(d2.deal_id as string), cast(d3.deal_id as string), cast(d7.deal_id as string)) as id_deal

  , case when c.rep_id != "" then coalesce(c.rep_id, cast(d2.owner_id as string), cast(d4.owner_id as string),cast(d.owner_id as string), cast(d5.owner_id as string), cast(d6.owner_id as string), cast(d3.owner_id as string)) else coalesce(cast(d2.owner_id as string), cast(d4.owner_id as string),cast(d.owner_id as string), cast(d5.owner_id as string), cast(d6.owner_id as string), cast(d3.owner_id as string)) end as id_owner
  , s.funnel_id
  , s.funnel_name
  FROM `bbg-platform.stripe_mindmint.payment_intent` pi

  LEFT JOIN mindmint_charge c ON pi.id = c.payment_intent_id
  LEFT JOIN `bbg-platform.stripe_mindmint.invoice` i ON pi.id = i.payment_intent_id
  LEFT JOIN `bbg-platform.stripe_mindmint.invoice_line_item` il ON i.id = il.invoice_id
  LEFT JOIN `bbg-platform.stripe_mindmint.price` pr ON pi.description = pr.nickname
  LEFT JOIN `bbg-platform.stripe_mindmint.product` pro ON pr.product_id = pro.id
LEFT JOIN mindmint_emails cu ON pi.customer_id = cu.id_customer

  LEFT JOIN dim_email e on cu.email = e.email_all

  LEFT JOIN mindmint_subs s ON i.subscription_id = s.id
  LEFT JOIN `hubspot2.product` p
    on c.object_id = cast(p.id as string)
  LEFT JOIN `hubspot2.deal` d
    on c.deal_id = cast(d.deal_id as string)
  LEFT JOIN `hubspot2.deal` d2
    on concat(coalesce(p.property_product_id, pro.id), email_all) = concat(d2.property_product_id, d2.property_email_address_of_contact)
  LEFT JOIN `bbg-platform.hubspot2.deal_pipeline_stage` ps
  ON coalesce(cast(d.deal_pipeline_stage_id as string),cast(d2.deal_pipeline_stage_id as string)) = ps.stage_id
  LEFT JOIN `hubspot2.product` p2
    on pr.id = p2.property_pricing_id
  LEFT JOIN `hubspot2.deal` d3
    on concat(c.object_id, email_all) = concat(d3.property_product_id, d3.property_email_address_of_contact)
  LEFT JOIN `hubspot2.deal` d4
   on lower(concat(
    case when coalesce(p.property_pricing_id, il.price_id, pr.id) = 'MBA_affirm_pp_7997' then 'MBA_Affirm_pp_9497' 
         when coalesce(p.property_pricing_id, il.price_id, pr.id) = 'MBA_plus_pif_15494_1' then 'MBA_Affirm_pp_15494' 
   else coalesce(p.property_pricing_id, il.price_id, pr.id) end, email_all)) = lower(concat(d4.property_pricing_id, d4.property_email_address_of_contact))  
  LEFT JOIN `hubspot2.deal` d5
    on pi.customer_id = d5.property_stripe_customer_id 
  LEFT JOIN `hubspot2.deal` d6
    on i.subscription_id = d6.property_subscription_id 
   left join deals d7
     on concat(e.email_prime,coalesce(p.property_product_id,pro.id)) = concat(d7.email,d7.id_product)
   
   
  -- left join `dbt_tscrivo.dim_email` e
  --   on email = e.email_all
  WHERE TRUE
  --  and pi.id = 'pi_3PwqRbLYbD2uWeLi1UBs5C78'
  --  and analytics.fnEmail_IsTest(cu.email) = false

  )

  select b.id_payment_intent
  , b.id_customer
  , b.status_charge
  , b.status_invoice
  , b.status_payment_intent
  , date_pi_created
  , email as email_trx
  , email_prime as email_primary
  , b.id_charge
  , b.id_invoice
  , b.id_price
  , b.status_subscription
  , b.stripe_account

    ,   case when email_prime is null then "no_contact" when coalesce(cast(m.deal_id as string), cast(b.id_deal as string)) is not null then coalesce(cast(m.deal_id as string), cast(b.id_deal as string)) when
  CASE 
    WHEN REGEXP_CONTAINS(
      email, 
      r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.(com|net|org|edu|co.uk|life|me|com.au|us|de)$'
    ) THEN TRUE
    ELSE FALSE
  END = false then "invalid_email" else "no_deal" end AS deal
  , coalesce(cast(d.owner_id as string), cast(b.id_owner as string)) as id_owner
   , coalesce(cast(m.deal_id as string), cast(b.id_deal as string)) as id_deal
   , DATETIME(d.property_closedate, 'America/Phoenix') as date_closed
   , dense_rank() over(partition by coalesce(cast(m.deal_id as string), cast(b.id_deal as string)) order by date_pi_created asc) as trans_num
   , dense_rank() over(partition by coalesce(cast(m.deal_id as string), cast(b.id_deal as string)) order by date_pi_created desc) as recency
   , d.property_pricing_id as id_price_hubspot
   , case when d.property_pricing_id = b.id_price then 1 else 0 end as is_hubspot_product_match
   , b.id_product
   , funnel_id
   , funnel_name
   , d.property_hubspot_team_id
  from base b
  left join `hubspot2.merged_deal` m
    on cast(b.id_deal as string) = cast(m.merged_deal_id as string)
  left join `hubspot2.deal` d
    on coalesce(cast(m.deal_id as string), cast(b.id_deal as string)) = cast(d.deal_id as string)
 -- where analytics.fnEmail_IsTest(email) = false
 --    and extract(year from date_pi_created) = 2024
   --    and status_charge = 'succeeded'
  --   and coalesce(cast(m.deal_id as string), cast(b.id_deal as string)) is not null
   --  and coalesce(b.id_owner, cast(d.owner_id as string)) is null
  -- and b.email = 'princepetra@yahoo.com'
  --     and (status_payment_intent = 'failed' or status_charge = 'failed')
   --    and email = 'info@thewellnessfix.com' #2 starter deals
  -- and email = 'carolynzacharias@aol.com'
 --  and name_product not in ('WORDPRESS_V1','wordpress','WHATSAPP_V1')
 --  and coalesce(cast(m.deal_id as string), cast(b.id_deal as string)) = '12165329876'
 -- and name_product like "%Mastermind Business%"
--and funnel_id = '13216474'
 --  where id_charge = 'ch_3Q5y62LYbD2uWeLi0CnTVqv3'
 -- and stripe_account = "BBG"
qualify row_number() over(partition by id_payment_intent) = 1