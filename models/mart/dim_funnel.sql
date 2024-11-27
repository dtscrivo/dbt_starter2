{{ config(materialized='table') }}

with base as (
with base as (
SELECT  json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as funnel_id
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as funnel_name
FROM `bbg-platform.stripe_mastermind.subscription_history` s
where cast(_fivetran_end as string) LIKE "9999%"

UNION ALL

  SELECT  json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as funnel_id
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as funnel_name
FROM `bbg-platform.stripe_mastermind.subscription_history` s
where cast(_fivetran_end as string) LIKE "9999%"
) 
select distinct base.funnel_id
  , base.funnel_name
from base
)



SELECT distinct funnel_step_id
  , c.funnel_id
  , coalesce(b.funnel_name, f.name)
  , max(collector_tstamp)

FROM `snowplow-348319.dbt_production.m_stg_conversions` c
LEFT JOIN `analytics.dim_funnels` f
  on cast(c.funnel_id as string) = f.funnel_id
LEFT JOIN base b
  on cast(c.funnel_id as string) = b.funnel_id
WHERE TRUE
  -- and analytics.fnEmail_IsTest(email) = false
  -- and TIMESTAMP_TRUNC(collector_tstamp, DAY) >= TIMESTAMP("2024-11-01")
  -- and se_action = 'optin'
  -- and funnel_id = 13529854
  GROUP BY all