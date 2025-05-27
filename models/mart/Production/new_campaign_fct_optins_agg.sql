l{{ config(materialized='table') }}

SELECT *
FROM `snowplow-348319.dbt_production.campaign_fct_optins_agg`