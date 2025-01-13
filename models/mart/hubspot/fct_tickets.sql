{{ config(materialized='table') }}

select *
from dbt_tscrivo.fct_hs_tickets