{{ config(materialized='table') }}

SELECT d.deal_id as id_deal
  , sum(property_amount) as amount_hs_deal
  , property_product_name as name_product
  , analytics.fnEmail(property_email_address_of_contact) as email
  , date(property_wire_date, 'America/Phoenix') as date
  , property_total_wire_amount as amount_wire
  , is_deleted
  , property_wire_payment_
  , e.email_prime
FROM `bbg-platform.hubspot2.deal` d
LEFT JOIN `bbg-platform.dbt_tscrivo.dim_email` e
  on analytics.fnEmail(property_email_address_of_contact) = e.email_all
LEFT JOIN `bbg-platform.hubspot2.merged_deal` m
   on d.deal_id = m.merged_deal_id
WHERE true
  and property_wire_payment_ = TRUE
--  and m.merged_deal_id IS NULL
  and d.is_deleted = FALSE
  group by all
