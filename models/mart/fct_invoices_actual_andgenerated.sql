{{ config(materialized='table') }}


WITH invoices as (
with asdf as (
  with initial_payments AS (
    SELECT
        analytics.fnEmail(email) as email,
        id_customer,
        id_price,
        date(date_pi_created) AS date_invoice,
        case when id_price IN ('MBA_pif_inpersonpackage_5997', 'bf22') then 1 else SAFE_CAST(pay_type AS INT64) end AS plan_type -- Using SAFE_CAST to avoid errors
        , amount_retained as amount_collected
        , name_product
        , id_deal
    FROM
        `dbt_tscrivo.fct_payment_intent_agg`
    WHERE
        num_payment = 1
        AND SAFE_CAST(pay_type AS INT64) IS NOT NULL -- Ensuring only valid integer values
        AND name_product not like "%@%"
)






, generated_payments AS (
    SELECT
        email,
        id_customer,
        id_price,
        date_invoice,
        plan_type,
        -- Generate each payment date using the SEQUENCE function
        DATE_ADD(date_invoice, INTERVAL n MONTH) AS payment_date,
        n + 1 AS payment_number
        , amount_collected
        , name_product
        , id_deal
    FROM
        initial_payments,
        UNNEST(GENERATE_ARRAY(0, plan_type - 1)) AS n
)
SELECT
    email,
    id_customer,
    id_price,
    payment_date,
    payment_number
    , "generated" as status_invoice
    , amount_collected
    , name_product
    , 1 as is_generated
    , id_deal
FROM
    generated_payments
 --   where email = 'adiyb@adiybmuhammad.com'
-- where id_price = 'taa_pp_lp_935_7'

ORDER BY
    email,
    id_price,
    payment_date
)


, base as (
SELECT email
  , id_customer
  , num_payment
  , id_price
  , date(date_pi_created) as payment_date
  , 0 as is_generated
  , status_charge
  , amount_collected
  , name_product
  , id_deal
FROM `dbt_tscrivo.fct_payment_intent_agg`
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
  , i.id_deal
FROM asdf i
WHERE true
  and date(payment_date) > current_date()
)


, first_payment as (
  select date(date_pi_created) as first_payment_date
  , email
  , id_price
  from `dbt_tscrivo.fct_payment_intent_agg`
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
  , id_deal
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
 --and status_charge = 'succeeded'
 --and id_deal = '16267352789'