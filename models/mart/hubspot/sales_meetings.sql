{{ config(
    materialized='table',
    schema='hubspot_sales'
) }}




SELECT *
FROM `bbg-platform.dbt_production_hubspot_engagements.sales_meetings`