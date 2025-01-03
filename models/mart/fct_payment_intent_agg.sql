{{ config(materialized='table') }}

       WITH mindmint_charge AS (
  WITH charges_with_refunds AS (
  SELECT 
    c.payment_intent_id,
    DATETIME(c.created, 'America/Phoenix') AS date_charge,
    c.status,
    ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created ASC) AS charge_num,
    ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created DESC) AS charge_success_num,
    c.id,
    c.outcome_reason,
    JSON_EXTRACT_SCALAR(c.metadata, "$.processor") AS processor,
    lower(COALESCE(c.statement_descriptor,c.calculated_statement_descriptor)) as statement_descriptor,
    c.metadata,
    JSON_EXTRACT_SCALAR(c.metadata, "$.product_id") AS object_id,
    JSON_EXTRACT_SCALAR(c.metadata, "$.rep_id") AS rep_id,
    c.refunded,
    COALESCE(CAST(m.deal_id AS STRING), JSON_EXTRACT_SCALAR(c.metadata, "$.deal_id")) AS deal_id,
    DATETIME(MAX(r.created), 'America/Phoenix') AS date_refund,  -- Max refund date
    SUM(r.amount/100) AS amount_refund,  -- Sum all refund amounts for this charge
    c.amount/100 AS amount_charge,
    CASE WHEN c.status = 'succeeded' THEN c.amount/100 ELSE NULL END AS amount_collected,
    d.status AS status_dispute,
    d.reason AS reason_dispute,
    DATETIME(d.created, 'America/Phoenix') AS date_dispute,
    case when d.status IN ("won","warning_closed") then null else d.amount/100 end AS amount_dispute
    , lower(cd.brand) as type_card
  FROM `bbg-platform.stripe_mindmint.charge` c
  LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
    ON CAST(JSON_EXTRACT_SCALAR(c.metadata, "$.deal_id") AS STRING) = CAST(m.merged_deal_id AS STRING)
  LEFT JOIN `bbg-platform.stripe_mindmint.refund` r
    ON c.id = r.charge_id
    and r.status = "succeeded"
  LEFT JOIN `bbg-platform.stripe_mindmint.dispute` d
    ON c.id = d.charge_id
  LEFT JOIN `bbg-platform.stripe_mindmint.card` cd
    ON c.payment_method_id = cd.id
 -- WHERE c.id = 'ch_3PS6nPLYbD2uWeLi1p24nBqM'
  GROUP BY 
    c.payment_intent_id, c.created, c.status, c.id, c.outcome_reason, c.metadata, c.amount, 
    c.refunded, m.deal_id, c.calculated_statement_descriptor, c.statement_descriptor, d.status, d.reason, d.created, d.amount, cd.brand
)
SELECT *
FROM charges_with_refunds
QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_intent_id ORDER BY date_charge DESC) = 1

  )

  , mastermind_charge AS (
WITH charges_with_refunds AS (
  SELECT 
    c.payment_intent_id,
    DATETIME(c.created, 'America/Phoenix') AS date_charge,
    c.status,
    ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created ASC) AS charge_num,
    ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created DESC) AS charge_success_num,
    c.id,
    c.outcome_reason,
    JSON_EXTRACT_SCALAR(c.metadata, "$.processor") AS processor,
    lower(COALESCE(c.statement_descriptor,c.calculated_statement_descriptor)) as statement_descriptor,
    c.metadata,
    JSON_EXTRACT_SCALAR(c.metadata, "$.product_id") AS object_id,
    JSON_EXTRACT_SCALAR(c.metadata, "$.rep_id") AS rep_id,

    c.refunded,
    COALESCE(CAST(m.deal_id AS STRING), JSON_EXTRACT_SCALAR(c.metadata, "$.deal_id")) AS deal_id,
    DATETIME(MAX(r.created), 'America/Phoenix') AS date_refund,  -- Max refund date
    SUM(r.amount/100) AS amount_refund,  -- Sum all refund amounts for this charge
    c.amount/100 AS amount_charge,
    CASE WHEN c.status = 'succeeded' THEN c.amount/100 ELSE NULL END AS amount_collected,
    d.status AS status_dispute,
    d.reason AS reason_dispute,
    DATETIME(d.created, 'America/Phoenix') AS date_dispute,
    case when d.status IN ("won","warning_closed") then null else d.amount/100 end AS amount_dispute
    , lower(cd.brand) as type_card
  FROM `bbg-platform.stripe_mastermind.charge` c
  LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
    ON CAST(JSON_EXTRACT_SCALAR(c.metadata, "$.deal_id") AS STRING) = CAST(m.merged_deal_id AS STRING)
  LEFT JOIN `bbg-platform.stripe_mastermind.refund` r
    ON c.id = r.charge_id
    and r.status = "succeeded"
  LEFT JOIN `bbg-platform.stripe_mastermind.dispute` d
    ON c.id = d.charge_id
  LEFT JOIN `bbg-platform.stripe_mastermind.card` cd
    ON c.payment_method_id = cd.id
  --WHERE c.id = 'ch_3PS6nPLYbD2uWeLi1p24nBqM'
  GROUP BY 
    c.payment_intent_id, c.created, c.status, c.id, c.outcome_reason, c.metadata, c.amount, 
    c.refunded, m.deal_id, c.calculated_statement_descriptor, c.statement_descriptor, d.status, d.reason, d.created, d.amount, cd.brand
)
SELECT *
FROM charges_with_refunds
QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_intent_id ORDER BY date_charge DESC) = 1

  )


, mindmint_subs as (
  SELECT s.id
  , DATETIME(s.created, 'America/Phoenix') as date_sub_created
  , s.status
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as funnel_id
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as funnel_name
  , CASE WHEN pause_collection_behavior IS NOT NULL then 1 else 0 end as is_collection_paused
  , DATETIME(s.pause_collection_resumes_at, 'America/Phoenix') as date_collection_pause_resumes
FROM `bbg-platform.stripe_mindmint.subscription_history` s
where cast(_fivetran_end as string) LIKE "9999%"
)

, mastermind_subs as (
  SELECT s.id
  , DATETIME(s.created, 'America/Phoenix') as date_sub_created
  , s.status
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as funnel_id
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as funnel_name
  , CASE WHEN pause_collection_behavior IS NOT NULL then 1 else 0 end as is_collection_paused
  , DATETIME(s.pause_collection_resumes_at, 'America/Phoenix') as date_collection_pause_resumes
FROM `bbg-platform.stripe_mastermind.subscription_history` s
where cast(_fivetran_end as string) LIKE "9999%"
)




-- , mastermind AS (
  SELECT
    pi.id AS id_payment_intent,
    pi.amount/100 as amount_pi,
    case when c.status = 'succeeded' then c.amount_charge else null end as amount_collected,
    c.date_charge,
    -- case when c.status = 'succeeded' then c.amount_refund else null end as amount_refund,
    c.amount_refund,
    c.date_refund,
    pi.customer_id AS id_customer,
    pi.description,
    pi.payment_method_id AS id_payment_method_id,
    pi.status AS status_payment_intent,
    c.statement_descriptor,
    DATETIME(pi.created, 'America/Phoenix') AS date_pi_created,
    c.charge_num,
    c.charge_success_num,
    c.status AS status_charge,
    i.status AS status_invoice,
    CASE
      WHEN c.status = "succeeded" AND charge_num = 1 THEN 'success'
      ELSE 'fail'
    END AS first_payment,
    CASE
      WHEN c.status IS NULL THEN "no-charge"
      WHEN c.status = "succeeded" AND charge_num = 1 THEN 'first_charge_success'
      WHEN c.status = "succeeded" AND charge_num != 1 THEN 'fail_recovered'
      ELSE "fail_not_recovered"
    END AS category,
    i.subscription_id AS id_subscription_invoice,
    analytics.fnEmail(cu.email) as email,
    cu.name,
    i.id AS id_invoice,

    c.id AS id_charge,
    i.hosted_invoice_url,
    i.invoice_pdf,
    il.price_id AS id_price,
    pr.unit_amount / 100 AS price,
    p.name AS name_product,
    p.id AS id_product,
    s.status AS status_subscription,
    -- date(s.date_sub_created) as date_sub_created,
    c.outcome_reason,
    pr.recurring_interval,
    s.id as id_subscription,
    -- DATETIME(i.next_payment_attempt, 'America/Phoenix') AS next_attempt
  case when c.status = 'succeeded' then (dense_rank() over(partition by email, p.id order by pi.created)) else null end as num_payment,
  case when c.status = 'succeeded' then (dense_rank() over(partition by email, p.id order by pi.created desc)) else null end as recency,
  dense_rank() over(partition by email, p.id, c.status order by pi.created) as num_attempt
  , "MM" as stripe_account
  , hp.property_hs_folder_name as group_product
  , DATETIME(property_closedate, 'America/Phoenix') as date_closed
  , ps.label as pipeline_stage
  , s.funnel_id
  , c.metadata
  , cast(d.deal_id as string) as id_deal
  -- , e.email_prime
  , d.property_createdate
  , right(il.price_id,1) as pay_type
  , c.amount_dispute
  , c.date_dispute
  ,  CASE
  -- When both `amount_refund` and `amount_dispute` are not null
  WHEN c.amount_refund IS NOT NULL AND c.amount_dispute IS NOT NULL THEN 
    c.amount_collected - c.amount_refund - c.amount_dispute

  -- When `amount_refund` is null and `amount_dispute` is not null
  WHEN c.amount_refund IS NULL AND c.amount_dispute IS NOT NULL THEN 
    c.amount_collected - c.amount_dispute

  -- When `amount_refund` is not null and `amount_dispute` is null
  WHEN c.amount_refund IS NOT NULL AND c.amount_dispute IS NULL THEN 
    c.amount_collected - c.amount_refund
  
  -- Else case: when both are null or other scenarios
  ELSE c.amount_collected
END as amount_retained
    , case when p.name LIKE "%The Action Academy%" then "TAA" 
         when p.name LIKE "%Mastermind Business Academy%" then "MBA"
         when p.name LIKE "%1:1 Coaching%" then "1:1"
         when p.name LIKE "%The Edge%" then "Edge"
         when p.name LIKE "%The Coaching Academy%" then "TCA"
         when p.name LIKE "%High Ticket Coaching%" then "HTC"
         when p.name LIKE "%High Ticket Academy%" then "HTA"
         else "" end as program

  , coalesce(c.rep_id, cast(d.owner_id as string)) as id_owner
  , c.type_card
  , s.is_collection_paused
  , s.date_collection_pause_resumes
  FROM `bbg-platform.stripe_mastermind.payment_intent` pi
  LEFT JOIN mastermind_charge c ON pi.id = c.payment_intent_id
  LEFT JOIN `bbg-platform.stripe_mastermind.invoice` i ON pi.id = i.payment_intent_id
  LEFT JOIN `bbg-platform.stripe_mastermind.invoice_line_item` il ON i.id = il.invoice_id
  LEFT JOIN `bbg-platform.stripe_mastermind.price` pr ON il.price_id = pr.id
  LEFT JOIN `bbg-platform.stripe_mastermind.product` p ON pr.product_id = p.id
  LEFT JOIN `bbg-platform.stripe_mastermind.customer` cu ON pi.customer_id = cu.id
  LEFT JOIN mastermind_subs s ON i.subscription_id = s.id
  LEFT JOIN `hubspot2.product` hp
    on p.id = cast(hp.property_product_id as string)
  LEFT JOIN `hubspot2.deal` d
    on c.deal_id = cast(d.deal_id as string)
LEFT JOIN `bbg-platform.hubspot2.deal_pipeline_stage` ps
  ON cast(d.deal_pipeline_stage_id as string) = ps.stage_id
-- left join `dbt_tscrivo.dim_email` e
--       on email = e.email_all

  WHERE TRUE
    and analytics.fnEmail_IsTest(cu.email) = false
  qualify row_number() over(partition by id_payment_intent order by property_createdate desc) = 1

UNION ALL


-- mindmint AS (

  SELECT
    pi.id AS id_payment_intent,
    pi.amount/100 as amount_pi,
    case when c.status = 'succeeded' then c.amount_charge else null end as amount_collected,
    c.date_charge,
    -- case when c.status = 'succeeded' then c.amount_refund else null end as amount_refund,
    c.amount_refund,
    c.date_refund,
    pi.customer_id AS id_customer,
    pi.description,
    pi.payment_method_id AS id_payment_method_id,
    pi.status AS status_payment_intent,
    c.statement_descriptor,
    DATETIME(pi.created, 'America/Phoenix') AS date_pi_created,
    c.charge_num,
    c.charge_success_num,
    c.status AS status_charge,
    case when i.status is null then "no invoice" else i.status end AS status_invoice,
    CASE
      WHEN c.status = "succeeded" AND charge_num = 1 THEN 'success'
      ELSE 'fail'
    END AS first_payment,
    CASE
      WHEN c.status IS NULL THEN "no-charge"
      WHEN c.status = "succeeded" AND charge_num = 1 THEN 'first_charge_success'
      WHEN c.status = "succeeded" AND charge_num != 1 THEN 'fail_recovered'
      ELSE "fail_not_recovered"
    END AS category,
    case when i.subscription_id is null then "no sub" else i.subscription_id end AS id_subscription_invoice,
    analytics.fnEmail(cu.email) as email,
    cu.name,
    case when i.id is null then "no invoice" else i.id end AS id_invoice,
    c.id AS id_charge,
    case when hosted_invoice_url is null then "no invoice" else i.hosted_invoice_url end as hosted_invoice_url ,
    case when i.invoice_pdf is null then "no invoice" else i.invoice_pdf end as invoice_pdf,
    coalesce(p.property_pricing_id, il.price_id) AS id_price,
    coalesce(p.property_price,pr.unit_amount/100) AS price,
    coalesce(p.property_name,pro.name) AS name_product,
    coalesce(p.property_product_id,pro.id) AS id_product,
    case when s.status is null then "no sub" else s.status end AS status_subscription,
    -- date('9999-12-31') as date_sub_created,
    c.outcome_reason,
    coalesce(p.property_recurringbillingfrequency,pr.recurring_interval) as recurring_interval,
    case when s.id is null then "no sub" else s.id end as id_subscription,
    -- "DATETIME(i.next_payment_attempt, 'America/Phoenix')" AS next_attempt
  case when c.status = 'succeeded' then (dense_rank() over(partition by email, coalesce(coalesce(p.property_product_id,pro.id),coalesce(p.property_pricing_id, il.price_id)), c.status order by pi.created)) else null end as num_payment,
  case when c.status = 'succeeded' then (dense_rank() over(partition by email, coalesce(coalesce(p.property_product_id,pro.id),coalesce(p.property_pricing_id, il.price_id)), c.status order by pi.created desc)) else null end as recency,
  dense_rank() over(partition by email, coalesce(coalesce(p.property_product_id,pro.id),coalesce(p.property_pricing_id, il.price_id)) order by pi.created) as num_attempt
  , "BBG" as stripe_account
  , coalesce(p.property_hs_folder_name, p2.property_hs_folder_name) as group_product
  -- , coalesce(DATETIME(d.property_closedate, 'America/Phoenix'),DATETIME(d2.property_closedate, 'America/Phoenix')) as date_closed
  , coalesce(DATE(d.property_closedate),DATE(d2.property_closedate)) as date_closed
  , ps.label as pipeline_stage
  , s.funnel_id
  , c.metadata
   , coalesce(cast(d4.deal_id as string), cast(d.deal_id as string),cast(d6.deal_id as string), cast(d2.deal_id as string), cast(d3.deal_id as string)) as id_deal
 -- , cast(d5.deal_id as string) as id_deal
  
  ,  d3.property_createdate
  -- , e.email_prime
  , case when lower(coalesce(p.property_pricing_id, il.price_id)) like "%affirm%" then '1' else right(coalesce(p.property_pricing_id, il.price_id),1)end as pay_type
  , c.amount_dispute
  , c.date_dispute
  ,  CASE
  -- When both `amount_refund` and `amount_dispute` are not null
  WHEN c.amount_refund IS NOT NULL AND c.amount_dispute IS NOT NULL THEN 
    c.amount_collected - c.amount_refund - c.amount_dispute

  -- When `amount_refund` is null and `amount_dispute` is not null
  WHEN c.amount_refund IS NULL AND c.amount_dispute IS NOT NULL THEN 
    c.amount_collected - c.amount_dispute

  -- When `amount_refund` is not null and `amount_dispute` is null
  WHEN c.amount_refund IS NOT NULL AND c.amount_dispute IS NULL THEN 
    c.amount_collected - c.amount_refund
  
  -- Else case: when both are null or other scenarios
  ELSE c.amount_collected
END as amount_retained
   , case when coalesce(p.property_name,pro.name) LIKE "%The Action Academy%" then "TAA" 
         when coalesce(p.property_name,pro.name) LIKE "%Mastermind Business Academy%" then "MBA"
         when coalesce(p.property_name,pro.name) LIKE "%1:1 Coaching%" then "1:1"
         when coalesce(p.property_name,pro.name) LIKE "%The Edge%" then "Edge"
         when coalesce(p.property_name,pro.name) LIKE "%The Coaching Academy%" then "TCA"
         when coalesce(p.property_name,pro.name) LIKE "%High Ticket Coaching%" then "HTC"
         when coalesce(p.property_name,pro.name) LIKE "%High Ticket Academy%" then "HTA"
         else null end as program

  , case when c.rep_id != "" then coalesce(c.rep_id, cast(d2.owner_id as string), cast(d4.owner_id as string),cast(d.owner_id as string), cast(d5.owner_id as string), cast(d6.owner_id as string), cast(d3.owner_id as string)) else coalesce(cast(d2.owner_id as string), cast(d4.owner_id as string),cast(d.owner_id as string), cast(d5.owner_id as string), cast(d6.owner_id as string), cast(d3.owner_id as string)) end as id_owner
  , c.type_card
  , s.is_collection_paused
  , s.date_collection_pause_resumes
  FROM `bbg-platform.stripe_mindmint.payment_intent` pi

  LEFT JOIN mindmint_charge c ON pi.id = c.payment_intent_id
  LEFT JOIN `bbg-platform.stripe_mindmint.invoice` i ON pi.id = i.payment_intent_id
  LEFT JOIN `bbg-platform.stripe_mindmint.invoice_line_item` il ON i.id = il.invoice_id
  LEFT JOIN `bbg-platform.stripe_mindmint.price` pr ON pi.description = pr.nickname
  LEFT JOIN `bbg-platform.stripe_mindmint.product` pro ON pr.product_id = pro.id
  LEFT JOIN `bbg-platform.stripe_mindmint.customer` cu ON pi.customer_id = cu.id

  LEFT JOIN `dbt_tscrivo.dim_email` e on cu.email = e.email_all

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
    on concat(c.object_id, email_prime) = concat(d3.property_product_id, d3.property_email_address_of_contact)
  LEFT JOIN `hubspot2.deal` d4
   on lower(concat(
    case when coalesce(p.property_pricing_id, il.price_id, pr.id) = 'MBA_affirm_pp_7997' then 'MBA_Affirm_pp_9497' 
         when coalesce(p.property_pricing_id, il.price_id, pr.id) = 'MBA_plus_pif_15494_1' then 'MBA_Affirm_pp_15494' 
   else coalesce(p.property_pricing_id, il.price_id, pr.id) end, email_prime)) = lower(concat(d4.property_pricing_id, d4.property_email_address_of_contact))  
  LEFT JOIN `hubspot2.deal` d5
    on pi.customer_id = d5.property_stripe_customer_id 
  LEFT JOIN `hubspot2.deal` d6
    on i.subscription_id = d6.property_subscription_id 
   
   
  -- left join `dbt_tscrivo.dim_email` e
  --   on email = e.email_all
  WHERE TRUE
  --  and pi.id = 'pi_3PwqRbLYbD2uWeLi1UBs5C78'
    and analytics.fnEmail_IsTest(cu.email) = false

  qualify row_number() over(partition by id_payment_intent order by property_createdate desc) = 1