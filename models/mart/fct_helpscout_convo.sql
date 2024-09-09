{{ config(materialized='table') }}

-- Latest Customer Update
with customer as (
  select h.id as id_customer
  , DATETIME(updated_at, 'America/Phoenix') as date_customer_updated
  , DATETIME(created_at, 'America/Phoenix') as date_customer_created
  , value as email_customer
  , location as location_customer
  from `bbg-platform.helpscout.customer_history` h
 LEFT JOIN `bbg-platform.helpscout.customer_email_history` e
   on h.id = e.customer_id
--where h.id = 712023528
qualify row_number() over(partition by customer_id order by customer_updated_at desc) = 1
)

-- latest user update
, user as (
  select id as id_user
  , email as email_user
  , timezone as user_timezone
  , first_name
  from `bbg-platform.helpscout.user_history`
 -- where id = 802904
qualify row_number() over(partition by id order by updated_at desc) = 1
)

-- Mailboxes: join on id_folder
, mailbox as (
  select m.id as id_mailbox 
  , f.id as id_folder
  , m.name as name_mailbox 
  , f.name as name_folder
  , f.type as type_folder
  , m.email as mailbox
  from `bbg-platform.helpscout.mailbox_history` m
  LEFT JOIN `bbg-platform.helpscout.mailbox_folder_history` f
  on m.id = f.mailbox_id
)

, threads as (
WITH ranked_threads AS (
  SELECT
    conversation_id,
    created_at,
    assigned_to_id, 
    source_via as thread_source,
    LEAD(created_at) OVER (
      PARTITION BY conversation_id 
      ORDER BY created_at
    ) AS next_created_at,
    LEAD(source_via) OVER (
      PARTITION BY conversation_id 
      ORDER BY created_at
    ) AS next_thread_source
  FROM `bbg-platform.helpscout.conversation_thread_history`
  WHERE true
    and source_via IN ('customer', 'user')
    and type in ('customer','message')
  --  AND conversation_id = 2701113180
)
SELECT
  conversation_id,
  assigned_to_id,
  DATETIME(created_at, 'America/Phoenix') AS customer_created_at,
  DATETIME(next_created_at, 'America/Phoenix') AS user_created_at,
  DATETIME_DIFF(next_created_at, created_at, MINUTE)/60 AS diff_in_minutes,
  row_number() over(partition by conversation_id order by created_at asc) as customer_thread_num,
  row_number() over(partition by conversation_id order by created_at desc) as customer_thread_recency
FROM ranked_threads
WHERE thread_source = 'customer'
  AND (next_thread_source = 'user' OR next_thread_source is null)
-- and conversation_id = 2701113180
)







, thread as (
SELECT h.id as id_thread
  , h.conversation_id
  , h.action_type
  , datetime(h.created_at, 'America/Phoenix') as date_thread
  , h.opened_at as date_thread_open
  , h.body
-- for thread source: "customer" and "user"
  , h.source_via as thread_source
  , h.state as thread_states
-- thread status: null if not a message
  , h.status as status_thread
-- for the type:
-- "customer" is a message from the customer
-- "message" is a message from the employee
  , h.type as type_thread

  , h.conversation_updated_at as date_conversation_updated_thread
  -- , u.email_user as closed_by
  -- , cu.email_customer
  -- , uu.first_name as assigned_to
  , h.assigned_to_id
  , row_number() over(partition by h.conversation_id, h.type order by h.created_at asc)
  , case when h.type = 'customer' then row_number() over(partition by h.conversation_id, h.type order by h.created_at asc) else null end as message_num_customer
  , case when h.type = 'message' then row_number() over(partition by h.conversation_id, h.type order by h.created_at asc) else null end as message_num_user
FROM `bbg-platform.helpscout.conversation_thread_history` h
-- LEFT JOIN user u
--   on c.closed_by = u.id_user
-- LEFT JOIN customer cu
--   on c.primary_customer_id = cu.id_customer
-- LEFT JOIN user uu
--   on h.assigned_to_id = uu.id_user
WHERE h.type in ('customer','message')
 -- and h.conversation_id = 858142865
order by h.conversation_id desc
)

select --c.*,
   datetime(c.updated_at, 'America/Phoenix') as updated_at
  , datetime(c.closed_at, 'America/Phoenix') as closed_at
  , datetime(c.created_at, 'America/Phoenix') as created_at
  , c.number
  , c.source_via
  , c.source_type
  , c.state
  , c.status
  , threads
  -- , bc.date_thread as first_cust
  -- , extract(dayofweek from bc.date_thread) as first_cust_day
  -- , bu.date_thread as first_user
  -- , extract(dayofweek from bu.date_thread) as first_user_day
  -- , date_diff(bu.date_thread,bc.date_thread,minute)/60 as first_response
  , m.name_mailbox
  , m.name_folder
  , m.type_folder
  , c.id
  , th.diff_in_minutes as response_time
  , date_diff(c.closed_at, c.created_at, day) as days_to_close
  -- , case when customer_thread_num = 1 then user_created_at else null end as user_created_at
  -- , case when customer_thread_num = 1 then customer_created_at else null end as customer_created_at
  -- , date_diff(case when customer_thread_num = 1 then user_created_at else null end,case when customer_thread_num = 1 then customer_created_at else null end,minute)/60
  , th.user_created_at as first_user_response
  , extract(dayofweek from th.user_created_at) as first_user_day
  , th.customer_created_at as first_customer_message
  , extract(dayofweek from th.customer_created_at) as first_customer_day
  , avg(th2.diff_in_minutes) as avg_response_time
  , th3.customer_created_at as last_customer_message
  , th3.user_created_at as last_user_response
  , case when th3.user_created_at is null then 0 else 1 end as is_responded
 -- , th2.customer_thread_recency
--  , u.first_name as assigned_to
  , case when u.email_user is not null or (first_name is null AND closed_at is not null) then 1 else 0 end as is_support_handled
  , case when first_name is null then 'Unassigned' else first_name end as assigned_to
  , case when first_name is not null then 1 else 0 end as is_assigned
  , case when closed_at is not null then 1 else 0 end as is_closed
  , case when source_type = 'beacon-v2' then 1 else 0 end as is_beacon 
  , m.mailbox
  , cu.email_customer
  , case when cu.email_customer like '%@replies.mastermind.com' or cu.email_customer like "%@deangraziosi.com" or cu.email_customer like "%@mastermind.com" then 1 else 0 end as is_notification
from `bbg-platform.helpscout.conversation_history` c
-- left join thread bc
--   on c.id = bc.conversation_id
--   and bc.message_num_customer = 1
-- left join thread bu
--   on c.id = bu.conversation_id
--   and bu.message_num_user = 1
left join mailbox m
  on c.folder_id = m.id_folder
left join threads th
  on c.id = th.conversation_id
  and customer_thread_num = 1
left join threads th2
  on c.id = th2.conversation_id
left join threads th3
  on c.id = th3.conversation_id
  and th3.customer_thread_recency = 1
left join user u
  on th3.assigned_to_id = u.id_user
left join customer cu
  on c.primary_customer_id = cu.id_customer
where true
 and date(c.created_at) > date('2023-12-31')
 and source_via = 'customer'

 -- and c.number = 408274
 -- and c.number = 1262526
 -- and c.id = 2701113180
--  and source_via = 'customer'
-- and name_mailbox = 'DG/ Mastermind.com Customer Support'
-- and name_folder != 'Notifications'
 -- and name_folder = 'Spam'
-- and th.user_created_at is not null
 group by all
qualify row_number() over(partition by number order by updated_at desc) = 1