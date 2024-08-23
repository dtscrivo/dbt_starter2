{{ config(materialized='table') }}

WITH invoices as (
WITH base as (
SELECT email
  , id_customer
  , num_payment
  , id_price
  , date(date_pi_created) as payment_date
  , 0 as is_generated
  , status_charge
  , amount_charge as amount_collected
  , name_product
FROM `dbt_tscrivo.fct_deal_payments`
WHERE name_product NOT LIKE "%@%"
 -- and email = 'Egyn98grl@hotmail.com'

UNION ALL

SELECT i.email
  , i.id_customer
  , i.payment_number as num_payment
  , i.id_price
  , i.payment_date
  , 1 as is_generated
  , i.status_invoice as status_charge
  , i.amount_collected
  , i.name_product

FROM `dbt_tscrivo.fct_invoice_generated` i
WHERE true
  and date(payment_date) > current_date()
)


, first_payment as (
  select date(date_pi_created) as first_payment_date
  , email
  , id_price
  from `dbt_tscrivo.fct_deal_payments`
  where num_payment = 1
)

SELECT b.email
  , b.id_price
  , b.amount_collected
  , b.payment_date
  , f.first_payment_date
  , b.num_payment
  , b.is_generated
  , b.status_charge
FROM base b
LEFT JOIN first_payment f
  on concat(b.email, b.id_price) = concat(f.email, f.id_price) 

where true
  and analytics.fnEmail_IsTest(b.email) = false
GROUP BY ALL
qualify ROW_NUMBER() OVER (PARTITION BY email, id_price, num_payment ORDER BY is_generated ASC) = 1
order by email, id_price, num_payment
)

SELECT *
FROM invoices
where true
  and analytics.fnEmail_IsTest(email) = false
 -- and (lower(id_price) like "%mba%" or lower(id_price) like "%taa%")
GROUP BY ALL
ORDER BY email, id_price, num_payment