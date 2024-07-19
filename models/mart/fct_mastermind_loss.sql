{{ config(materialized='table') }}

SELECT c.id as charge_id
  , datetime(c.created, 'America/Phoenix') as date_charge
  , datetime(e.created, 'America/Phoenix') as date_fraud_warning
  , datetime(d.created, 'America/Phoenix') as date_dispute
  , datetime(bt.created, 'America/Phoenix') as date_balance_trans
  , datetime(r.created, 'America/Phoenix') as date_refund
  , coalesce(date_diff(d.created,e.created,day), 9999) as dob_warning_to_dispute
  , coalesce(date_diff(r.created,c.created,day), 9999) as dob_refund
  , coalesce(r.amount/100, d.amount/100) as amount
  , d.metadata
  , d.status as status_dispute
  , d.reason as reason_dispute
  , e.fraud_type
  , e.actionable
  , bt.fee / 100 as fee
  , c.amount / 100 as amount_charge
  , d.amount / 100 as amount_dispute
  , r.amount / 100 as amount_refund
  , bt.description
  , bt.net / 100 as net
  , bt.reporting_category
  , bt.status as status_balance_
  , bt.type as type_balance_trans
  , analytics.fnEmail(cu.email) as email
  , pr.id as id_pricing
  , p.name as product
  , pr.unit_amount / 100 as price
  , pr.type as type_product
  , case when d.created is not null then 1 else 0 end as is_dispute
  , case when d.created is not null and d.status = "lost" then 1 else 0 end as is_dispute_lost
  , case when d.created is not null and d.status = "won" then 1 else 0 end as is_dispute_won
  , case when d.created is not null and d.status = "needs_response" then 1 else 0 end as is_dispute_needs_response
  , case when d.created is not null and d.status = "under_review" then 1 else 0 end as is_dispute_under_review
  , case when d.created is not null and d.status = "warning_closed" then 1 else 0 end as is_dispute_closed
  , case when e.created is not null then 1 else 0 end as is_fraud_warning
  , case when r.created is not null then 1 else 0 end as is_refund
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
LEFT JOIN `bbg-platform.stripe_mastermind.refund` r
  on c.id = r.charge_id
WHERE c.status = "succeeded"
  and (d.created IS NOT NULL or e.created is not null or r.created is not null)
 -- and cu.email = "eviegonline@gmail.com"
--  and date(c.created) >= date('2023-01-01')
qualify row_number() over(partition by c.id) = 1