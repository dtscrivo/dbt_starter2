{{ config(materialized='table') }}

with subs AS(
SELECT s.id
  , TIMESTAMP_SUB(s.created, INTERVAL 7 HOUR) as date_sub_created
  , s.status
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as funnel_id
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as funnel_name
FROM `bbg-platform.stripe_mindmint.subscription_history` s
where cast(_fivetran_end as string) LIKE "9999%"
--qualify row_number() over(partition by s.id order by created desc) = 1
)

SELECT i.id as id_invoice
  , amount_paid as amount_collected
  , i.status as status_invoice
  , i.subscription_id as id_subscription
  , cu.email
  , cu.name
  , i.id as id_charge
  , hosted_invoice_url
  , invoice_pdf
  , il.price_id as id_price
  , p.name as name_product
  , p.id as id_product
  , s.status as status_subscription
  , s.date_sub_created
  , pr.recurring_interval
 -- , s.funnel_id as id_funnel
 -- , s.funnel_name as name_funnel
  , row_number() over(partition by cu.email, il.price_id order by i.created asc) as num_payment
 -- , right(il.price_id,1) as plan_type
  , CASE 
    -- First condition: check if the name contains "@"
    WHEN p.name LIKE "%@%" THEN '1'
    ELSE
      CASE
        -- Check if both of the last two characters are numeric
        WHEN REGEXP_CONTAINS(SUBSTR(il.price_id, -2), r'^\d{2}$') THEN SUBSTR(il.price_id, -2)
        -- If one or both are non-numeric, take the last character only
        ELSE SUBSTR(il.price_id, -1)
      END
  END AS plan_type
 -- , s.id as id_subscription
  , TIMESTAMP_SUB(i.created, INTERVAL 7 HOUR) as date_invoice
FROM `bbg-platform.stripe_mindmint.invoice` i 
LEFT JOIN `bbg-platform.stripe_mindmint.invoice_line_item` il
  on i.id = il.invoice_id
LEFT JOIN `bbg-platform.stripe_mindmint.price` pr
  on il.price_id = pr.id
LEFT JOIN `bbg-platform.stripe_mindmint.product` p
  on pr.product_id = p.id
LEFT JOIN `bbg-platform.stripe_mindmint.customer` cu
  on i.customer_id = cu.id
LEFT JOIN subs s
  on i.subscription_id = s.id
--WHERE p.name NOT LIKE "%@%"