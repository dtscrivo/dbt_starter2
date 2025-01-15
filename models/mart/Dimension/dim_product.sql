{{ config(
    materialized='table',
    schema='dimension'
) }}

SELECT mm.id as id_product_stripe
  , mm.active as is_active
  , DATETIME(mm.created, 'America/Phoenix') as date_created_stripe
  , mm.description as description_stripe
  , mm.is_deleted
  , mm.name as name_product_stripe
  , mm.statement_descriptor
  , mm.type
  , mm.metadata as metadata_stripe
  , p.id as id_product_hubspot
  , p.portal_id as id_portal
  , p.property_createdate as date_created_hubspot
  , p.property_description as description_hubspot
  , p.property_name as name_product_hubspot
  , p.property_price as price_hubspot
  , p.property_pricing_id as id_price_hubspot
  , p.property_recurringbillingfrequency as type_billing
  , p.property_hs_object_source as source_creation_hubspot
  , p.property_hs_folder_name as group_product_hubspot
  , pr.id as id_price_stripe
  , pr.active as is_active_stripe
  , pr.recurring_interval as type_billing_stripe
  , pr.unit_amount/100 as price_stripe
  , "mastermind" as account
FROM `bbg-platform.stripe_mastermind.price` pr
LEFT JOIN `bbg-platform.stripe_mastermind.product` mm
  on cast(mm.id as string) = pr.product_id
LEFT JOIN `hubspot2.product` p
  on pr.id = p.property_pricing_id

WHERE mm.name not like "%@%"

UNION ALL

SELECT mm.id as id_product_stripe
  , mm.active as is_active
  , DATETIME(mm.created, 'America/Phoenix') as date_created_stripe
  , mm.description as description_stripe
  , mm.is_deleted
  , mm.name as name_product_stripe
  , mm.statement_descriptor
  , mm.type
  , mm.metadata as metadata_stripe
  , p.id as id_product_hubspot
  , p.portal_id as id_portal
  , p.property_createdate as date_created_hubspot
  , p.property_description as description_hubspot
  , p.property_name as name_product_hubspot
  , p.property_price as price_hubspot
  , p.property_pricing_id as id_price_hubspot
  , p.property_recurringbillingfrequency as type_billing
  , p.property_hs_object_source as source_creation_hubspot
  , p.property_hs_folder_name as group_product_hubspot
  , pr.id as id_price_stripe
  , pr.active as is_active_stripe
  , pr.recurring_interval as type_billing_stripe
  , pr.unit_amount/100 as price_stripe
  , "mindmint" as account
FROM `bbg-platform.stripe_mindmint.price` pr
LEFT JOIN `bbg-platform.stripe_mindmint.product` mm
  on cast(mm.id as string) = pr.product_id
LEFT JOIN  `hubspot2.product` p
  on pr.id = p.property_pricing_id

WHERE mm.name not like "%@%"