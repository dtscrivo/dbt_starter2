{{ config(materialized='table') }}

  SELECT "MM" as stripe_account
  , s.id as id_subscription
  , DATETIME(s.created, 'America/Phoenix') as date_sub_created
  , s.status
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as id_funnel
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as name_funnel
  , DATETIME(s.canceled_at, 'America/Phoenix') as date_canceled
  , DATETIME(s.current_period_end, 'America/Phoenix') as date_period_end
  , DATETIME(s.current_period_start, 'America/Phoenix') as date_period_start
  , s.customer_id as id_customer
  , s.latest_invoice_id as id_latest_invoice
  , json_extract_scalar(s.metadata, "$.connected_account") as connected_account
  , case when s.pause_collection_behavior IS NOT NULL then 1 else 0 end as is_collection_paused
  , DATETIME(s.pause_collection_resumes_at, 'America/Phoenix') as date_pause_resumes
  , DATETIME(s.start_date, 'America/Phoenix') as date_sub_start
  , DATETIME(s.trial_end, 'America/Phoenix') as date_trial_end
  , DATETIME(s.trial_start, 'America/Phoenix') as date_trial_start
  , s.cancellation_details_reason
  , case when s.cancellation_details_reason IS NOT NULL then 1 else 0 end as is_cancelled
  , case when s.cancellation_details_reason = 'payment_failed' then 1 else 0 end as is_cancel_by_payment
  , case when s.cancellation_details_reason = 'cancellation_requested' then 1 else 0 end as is_cancel_requested
  , analytics.fnEmail(c.email) as email
  , case when analytics.fnEmail_IsTest(c.email) = TRUE then 1 else 0 end as is_test
FROM `bbg-platform.stripe_mastermind.subscription_history` s
LEFT JOIN `stripe_mastermind.customer` c
  on s.customer_id = c.id
where cast(_fivetran_end as string) LIKE "9999%"
qualify row_number() over(partition by s.id) = 1

UNION ALL

  SELECT "BBG" as stripe_account
  , s.id as id_subscription
  , DATETIME(s.created, 'America/Phoenix') as date_sub_created
  , s.status
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as id_funnel
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as name_funnel
  , DATETIME(s.canceled_at, 'America/Phoenix') as date_canceled
  , DATETIME(s.current_period_end, 'America/Phoenix') as date_period_end
  , DATETIME(s.current_period_start, 'America/Phoenix') as date_period_start
  , s.customer_id as id_customer
  , s.latest_invoice_id as id_latest_invoice
  , json_extract_scalar(s.metadata, "$.connected_account") as connected_account
  , case when s.pause_collection_behavior IS NOT NULL then 1 else 0 end as is_collection_paused
  , DATETIME(s.pause_collection_resumes_at, 'America/Phoenix') as date_pause_resumes
  , DATETIME(s.start_date, 'America/Phoenix') as date_sub_start
  , DATETIME(s.trial_end, 'America/Phoenix') as date_trial_end
  , DATETIME(s.trial_start, 'America/Phoenix') as date_trial_start
  , s.cancellation_details_reason
  , case when s.cancellation_details_reason IS NOT NULL then 1 else 0 end as is_cancelled
  , case when s.cancellation_details_reason = 'payment_failed' then 1 else 0 end as is_cancel_by_payment
  , case when s.cancellation_details_reason = 'cancellation_requested' then 1 else 0 end as is_cancel_requested
  , analytics.fnEmail(c.email) as email
  , case when analytics.fnEmail_IsTest(c.email) = TRUE then 1 else 0 end as is_test
FROM `bbg-platform.stripe_mindmint.subscription_history` s
LEFT JOIN `stripe_mastermind.customer` c
  on s.customer_id = c.id
where cast(_fivetran_end as string) LIKE "9999%"
qualify row_number() over(partition by s.id) = 1