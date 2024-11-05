{{ config(materialized='table') }}

with trans as (
  SELECT c.id as id_charge
  , datetime(c.created, 'America/Phoenix') as date_charge
  , datetime(e.created, 'America/Phoenix') as date_fraud_warning
  , datetime(d.created, 'America/Phoenix') as date_dispute
  , datetime(r.created, 'America/Phoenix') as date_refund
  , coalesce(date_diff(d.created,e.created,day), 9999) as dob_warning_to_dispute
  , c.metadata
  , d.status as status_dispute
  , d.reason as reason_dispute
  , e.fraud_type
  , e.actionable
  , r.amount / 100 as amount_refund
  , bt.fee / 100 as amount_fee
  , d.amount / 100 as amount_dispute
  , c.amount / 100 as amount_charge
  , bt.created as date_balance_trans
  , bt.description
  , bt.net / 100 as amount_trans_net
  , bt.reporting_category
  , bt.status as status_balance
  , bt.type as type_balance_trans
  , analytics.fnEmail(cu.email) as email
  , pr.id as id_price
  , p.name as product
  , pr.unit_amount / 100 as amount_price
  , pr.type
  , case when d.created is not null then 1 else 0 end as is_dispute
  , case when d.created is not null and d.status = "lost" then 1 else 0 end as is_dispute_lost
  , case when d.created is not null and d.status = "won" then 1 else 0 end as is_dispute_won
  , case when d.created is not null and d.status = "needs_response" then 1 else 0 end as is_dispute_needs_response
  , case when d.created is not null and d.status = "under_review" then 1 else 0 end as is_dispute_under_review
  , case when d.created is not null and d.status = "warning_closed" then 1 else 0 end as is_dispute_closed
  , case when e.created is not null then 1 else 0 end as is_fraud_warning
  , d.id as id_dispute
  , il.invoice_id as id_invoice
  , dense_rank() over(partition by c.id order by d.created asc) as num_dispute
  , "MM" as stripe_account
  , case when c.refunded = true then 1 else 0 end as is_refund
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_id") as funnel_id
  , json_extract_scalar(s.metadata, "$.netsuite_CF_funnel_name") as funnel_name
  , lower(cd.brand) as type_card
  , lower(COALESCE(c.statement_descriptor,c.calculated_statement_descriptor)) as statement_descriptor
  , DATETIME(pi.created, 'America/Phoenix') as date_payment_intent
  , pi.status as status_pi
  , i.status as status_invoice
  , r.status as status_refund
FROM `bbg-platform.stripe_mastermind.charge` c
LEFT JOIN `bbg-platform.stripe_mastermind.dispute` d
  on c.id = d.charge_id
LEFT JOIN `bbg-platform.stripe_mastermind.early_fraud_warning` e
  on c.id = e.charge_id
LEFT JOIN `bbg-platform.stripe_mastermind.dispute_balance_transaction` b
  on d.id = b.dispute_id
LEFT JOIN `bbg-platform.stripe_mastermind.balance_transaction` bt
  on d.balance_transaction = bt.id
LEFT JOIN `bbg-platform.stripe_mastermind.customer` cu
  on c.customer_id = cu.id
LEFT JOIN `bbg-platform.stripe_mastermind.invoice_line_item` il
  on c.invoice_id = il.invoice_id
LEFT JOIN `bbg-platform.stripe_mastermind.price` pr
  on il.price_id = pr.id
LEFT JOIN `bbg-platform.stripe_mastermind.product` p
  on pr.product_id = p.id
LEFT JOIN `bbg-platform.stripe_mastermind.subscription_history` s
  on il.subscription_id = s.id
LEFT JOIN `bbg-platform.stripe_mastermind.refund` r
  on c.id = r.charge_id
  and r.status = 'succeeded'
LEFT JOIN `bbg-platform.stripe_mastermind.card` cd
  ON c.payment_method_id = cd.id
LEFT JOIN `bbg-platform.stripe_mastermind.payment_intent` pi
  ON c.payment_intent_id = pi.id
LEFT JOIN `bbg-platform.stripe_mastermind.invoice` i
  ON c.invoice_id = i.id
WHERE c.status = "succeeded"
 -- and (d.created IS NOT NULL or e.created is not null)
 -- and cu.email = "eviegonline@gmail.com"
 -- and date(d.created, 'America/Phoenix') = date('2024-09-16')
 -- and c.id = 'ch_3PaN4RISjDEJDDVR0GNXdMFV'
qualify row_number() over(partition by c.id, d.id) = 1

union all


  SELECT c.id as id_charge
  , datetime(c.created, 'America/Phoenix') as date_charge
  , datetime(e.created, 'America/Phoenix') as date_fraud_warning
  , datetime(d.created, 'America/Phoenix') as date_dispute
  , datetime(r.created, 'America/Phoenix') as date_refund
  , coalesce(date_diff(d.created,e.created,day), 9999) as dob_warning_to_dispute
  , c.metadata
  , d.status as status_dispute
  , d.reason as reason_dispute
  , e.fraud_type
  , e.actionable
  , r.amount / 100 as amount_refund
  , bt.fee / 100 as amount_fee
  , d.amount / 100 as amount_dispute
  , c.amount / 100 as amount_charge
  , bt.created as date_balance_trans
  , bt.description
  , bt.net / 100 as amount_trans_net
  , bt.reporting_category
  , bt.status as status_balance
  , bt.type as type_balance_trans
  , analytics.fnEmail(cu.email) as email
  , pr.id as id_price
  , p.name as product
  , pr.unit_amount / 100 as amount_price
  , pr.type
  , case when d.created is not null then 1 else 0 end as is_dispute
  , case when d.created is not null and d.status = "lost" then 1 else 0 end as is_dispute_lost
  , case when d.created is not null and d.status = "won" then 1 else 0 end as is_dispute_won
  , case when d.created is not null and d.status = "needs_response" then 1 else 0 end as is_dispute_needs_response
  , case when d.created is not null and d.status = "under_review" then 1 else 0 end as is_dispute_under_review
  , case when d.created is not null and d.status = "warning_closed" then 1 else 0 end as is_dispute_closed
  , case when e.created is not null then 1 else 0 end as is_fraud_warning
  , d.id as id_dispute
  , il.invoice_id as id_invoice
  , dense_rank() over(partition by c.id order by d.created asc) as num_dispute
  , "BBG" as stripe_account
  , case when c.refunded = true then 1 else 0 end as is_refund

  , ""
  , ""
  , lower(cd.brand) as type_card
  , lower(COALESCE(c.statement_descriptor,c.calculated_statement_descriptor)) as statement_descriptor
  , DATETIME(pi.created, 'America/Phoenix') as date_payment_intent
  , pi.status as status_pi
  , i.status as status_invoice
  , r.status as status_refund
FROM `bbg-platform.stripe_mindmint.charge` c
LEFT JOIN `bbg-platform.stripe_mindmint.dispute` d
  on c.id = d.charge_id
LEFT JOIN `bbg-platform.stripe_mindmint.early_fraud_warning` e
  on c.id = e.charge_id
LEFT JOIN `bbg-platform.stripe_mindmint.dispute_balance_transaction` b
  on d.id = b.dispute_id
LEFT JOIN `bbg-platform.stripe_mindmint.balance_transaction` bt
  on d.balance_transaction = bt.id
LEFT JOIN `bbg-platform.stripe_mindmint.customer` cu
  on c.customer_id = cu.id
LEFT JOIN `bbg-platform.stripe_mindmint.invoice_line_item` il
  on c.invoice_id = il.invoice_id
LEFT JOIN `bbg-platform.stripe_mindmint.price` pr
  on il.price_id = pr.id
LEFT JOIN `bbg-platform.stripe_mindmint.product` p
  on pr.product_id = p.id
LEFT JOIN `bbg-platform.stripe_mindmint.refund` r
  on c.id = r.charge_id
  and r.status = 'succeeded'
LEFT JOIN `bbg-platform.stripe_mindmint.card` cd
  ON c.payment_method_id = cd.id
LEFT JOIN `bbg-platform.stripe_mindmint.payment_intent` pi
  ON c.payment_intent_id = pi.id
LEFT JOIN `bbg-platform.stripe_mindmint.invoice` i
  ON c.invoice_id = i.id
WHERE c.status = "succeeded"
 -- and (d.created IS NOT NULL or e.created is not null)
 -- and cu.email = "eviegonline@gmail.com"
 -- and date(d.created, 'America/Phoenix') = date('2024-09-18')
 -- and c.id = 'ch_3PaN4RISjDEJDDVR0GNXdMFV'
qualify row_number() over(partition by c.id, d.id) = 1
)

select * from trans
where true
 -- and stripe_account = "MM"
 -- and date(date_dispute) = date('2024-09-19')
  and status_pi != "canceled"