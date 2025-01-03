{{ config(materialized='table') }}

SELECT deal_id as id_deal
  , sum(property_amount) as amount_hs_deal
  , property_product_name as name_product
  , analytics.fnEmail(property_email_address_of_contact) as email
  , date(property_wire_date, 'America/Phoenix') as date
  , property_total_wire_amount as amount_wire
FROM `bbg-platform.hubspot2.deal`
WHERE true
  and property_wire_payment_ = TRUE
group by all