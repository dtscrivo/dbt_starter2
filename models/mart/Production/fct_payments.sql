{{ config(materialized='table') }}

WITH base as (
select amount_collected as amount_gross
  , date_charge as date_transaction
  , id_deal
  , "charge" as type
  , amount_pi
  , id_payment_intent
  , name_product
  , email
  , id_price
  , status_charge
  , num_payment
  , category
  , stripe_account
  , id_invoice
  , recency
  , date_closed
  , program
  , type_card
  , statement_descriptor
from `bbg-platform.dbt_tscrivo.fct_payment_intent_agg`

union all

select -amount_dispute
  , date_dispute
  , id_deal
  , "dispute" as type
  , amount_pi
  , id_payment_intent
  , name_product
  , email
  , id_price
  , status_charge
  , num_payment
  , category
  , stripe_account
  , id_invoice
  , recency
  , date_closed
  , program  
  , type_card
  , statement_descriptor
from `bbg-platform.dbt_tscrivo.fct_payment_intent_agg`

union all

select -amount_refund
  , date_refund
  , id_deal
  , "refund" as type
  , amount_pi
  , id_payment_intent
  , name_product
  , email
  , id_price
  , status_charge
  , num_payment
  , category
  , stripe_account
  , id_invoice
  , recency
  , date_closed
  , program
  , type_card
  , statement_descriptor
from `bbg-platform.dbt_tscrivo.fct_payment_intent_agg`

union all

SELECT sum(property_amount) as amount_collected
    , datetime(property_closedate, 'America/Phoenix') as date_collected
       , cast(deal_id as string) as id_deal
      , "wire" as type
       , sum(property_amount_in_home_currency)
      , "wire" as id_payment_intent
       , property_product_name
       , analytics.fnEmail(property_email_address_of_contact) as email
       , property_pricing_id as id_price
         , "succeeded"
         , 1
         , "wire"
        , 'HS'
        , "wire"
        , 0
        , datetime(property_closedate, 'America/Phoenix')
        , case when coalesce(property_product_name) LIKE "%The Action Academy%" then "TAA" 
         when coalesce(property_product_name) LIKE "%Mastermind Business Academy%" then "MBA"
         when coalesce(property_product_name) LIKE "%1:1 Coaching%" then "1:1"
         when coalesce(property_product_name) LIKE "%The Edge%" then "Edge"
         when coalesce(property_product_name) LIKE "%The Coaching Academy%" then "TCA"
         when coalesce(property_product_name) LIKE "%High Ticket Coaching%" then "HTC"
         when coalesce(property_product_name) LIKE "%High Ticket Academy%" then "HTA"
         else null end as program
          , "wire" as type_card
  , "wire" as statement_descriptor
FROM `bbg-platform.hubspot2.deal`
WHERE true
  and property_wire_payment_ = TRUE
    group by all
)

SELECT b.*
  , coalesce(b.date_closed,b1.date_transaction) as date_order
FROM base b
LEFT JOIN base b1
  on concat(b.email, b.id_price) = concat(b1.email, b1.id_price)
  and b1.num_payment = 1
  and b1.status_charge = 'succeeded'
  and b1.date_transaction IS NOT NULL
WHERE true
  and b.email is not null
  and b.status_charge = 'succeeded'
  and b.date_transaction is not null
--  and b.id_payment_intent = 'pi_3Pb9gOISjDEJDDVR0oKLpFjA'
qualify row_number() over(partition by id_payment_intent, type, date_transaction) = 1