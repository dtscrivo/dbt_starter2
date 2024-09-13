{{ config(materialized='table') }}

-- Latest Customer Update
with customer as (
  select h.id as id_customer
  , DATETIME(updated_at, 'America/Phoenix') as date_customer_updated
  , DATETIME(created_at, 'America/Phoenix') as date_customer_created
--  , value as email_customer
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
  , first_name as name

  , case when t.team_id = 793820 then 'Account Changes'
when t.team_id = 789132 then 'Business Hub'
when t.team_id = 741056 then 'Dev Requests'
when t.team_id = 792871 then 'Enterprise'
when t.team_id = 796834 then 'GG'
when t.team_id = 798395 then 'KBB Workshops'
when t.team_id = 797971 then 'Legacy Builder'
when t.team_id = 801929 then 'MBA Students'
when t.team_id = 796896 then 'MM Login Help'
when t.team_id = 796996 then 'Notifications'
when t.team_id = 796690 then 'Obvio Links'
when t.team_id = 799135 then 'Plug N Play'
when t.team_id = 804598 then 'Point Issues'
when t.team_id = 722541 then 'Saves and Declines'
when t.team_id = 719557 then 'Shipping'
when t.team_id = 797096 then 'Tony Leadership Mastery'
when t.team_id = 799141 then 'World Summit'
when t.team_id = 720198 then 'Dev Requests'
when t.team_id = 792032 then 'Unassigned'
when t.team_id = 789553 then 'Unassigned'
when t.team_id = 789553 then 'MBA Students'
when t.team_id = 802907 then 'Saves and Declines'
when t.team_id = 802904 then 'Saves and Declines'
when t.team_id = 802905 then 'Saves and Declines'
when t.team_id = 802903 then 'MBA Students'
when t.team_id = 720197 then 'Saves and Declines'
when t.team_id = 720189 then 'MBA Students'
when t.team_id = 720184 then 'Saves and Declines'
when t.team_id = 800463 then 'Saves and Declines'
when t.team_id = 798573 then 'Saves and Declines'
when t.team_id = 720018 then 'Business Hub'
when t.team_id = 720186 then 'Business Hub'
when t.team_id = 792033 then 'KBB Workshops'
when t.team_id = 800462 then 'Business Hub'
when t.team_id = 800670 then 'Business Hub'
when t.team_id = 800464 then 'Business Hub'
when t.team_id = 798063 then 'Saves and Declines'
else u.first_name end as team
  from `bbg-platform.helpscout.user_history` u
  left join `bbg-platform.helpscout.team_user_history` t
  on u.id = t.user_id
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

-- groups threads by customer message and user response
, threads as (
WITH ranked_threads AS (
  SELECT
    conversation_id,
    created_at,
    status,
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
  DATETIME_DIFF(next_created_at, created_at, hour) AS response_time_hours,
  row_number() over(partition by conversation_id order by created_at asc) as customer_thread_num,
  row_number() over(partition by conversation_id order by created_at desc) as customer_thread_recency
  , status
FROM ranked_threads
WHERE thread_source = 'customer'
  AND (next_thread_source = 'user' OR next_thread_source is null)
-- and conversation_id = 2701113180
)

, cancels as (
SELECT
  id,
  sum(case when closed_at is not null then 1 else 0 end) as num_cancelled
FROM `helpscout.conversation_history`
GROUP BY ALL


-- and conversation_id = 2701113180
)


-- thread detail CTE
, thread as (
SELECT h.id as id_thread
  , h.conversation_id
  , h.status
  -- , h.action_type
  -- , datetime(h.created_at, 'America/Phoenix') as date_thread
  -- , h.opened_at as date_thread_open
  -- , h.body
-- for thread source: "customer" and "user"
  -- , h.source_via as thread_source
  -- , h.state as thread_states
-- thread status: null if not a message
  -- , h.status as status_thread
-- for the type:
-- "customer" is a message from the customer
-- "message" is a message from the employee
  -- , h.type as type_thread

  -- , h.conversation_updated_at as date_conversation_updated_thread
  -- , u.email_user as closed_by
  -- , cu.email_customer
  -- , uu.first_name as assigned_to
  -- , h.assigned_to_id
  , row_number() over(partition by h.conversation_id order by h.created_at asc) as message_num
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
  , datetime(c.closed_at, 'America/Phoenix') as date_convo_closed
  , datetime(c.created_at, 'America/Phoenix') as date_convo
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
  , c.id as id_conversation
  , th.response_time_hours
  , date_diff(c.closed_at, c.created_at, day) as days_to_close
  -- , case when customer_thread_num = 1 then user_created_at else null end as user_created_at
  -- , case when customer_thread_num = 1 then customer_created_at else null end as customer_created_at
  -- , date_diff(case when customer_thread_num = 1 then user_created_at else null end,case when customer_thread_num = 1 then customer_created_at else null end,minute)/60
  , th.user_created_at as first_user_response
  , extract(dayofweek from th.user_created_at) as first_user_day
  , th.customer_created_at as first_customer_message
  , extract(dayofweek from th.customer_created_at) as first_customer_day
  , avg(th2.response_time_hours) as avg_response_time
  , th3.customer_created_at as last_customer_message
  , th3.user_created_at as last_user_response
  , case when th3.user_created_at is null then 0 else 1 end as is_responded
 -- , th2.customer_thread_recency
--  , u.name as assigned_to
  , case when u.email_user is not null or (name is null AND closed_at is not null) then 1 else 0 end as is_support_handled
  , case when name is null then 'Unassigned' else name end as assigned_to
  , case when name is not null then 1 else 0 end as is_assigned
  , case when closed_at is not null then 1 else 0 end as is_closed
  , case when source_type = 'beacon-v2' then 1 else 0 end as is_beacon 
  , m.mailbox
  , coalesce(team, "Not Assigned") as team
  , max(bc.message_num) as messages
  , max(bc.message_num_customer) as customer_messages
  , max(bc.message_num_user) as user_messages
  , case when c.customer_waiting_since_time is not null and closed_at is not null then 0 else 1 end as is_waiting
  , x.num_cancelled
  , case when c.status = "closed" AND max(bc.message_num_customer) = 1 then 1 else 0 end as is_first_closed
--  , cu.email_customer
--  , case when cu.email_customer like 'info@%' or cu.email_customer like "%systemmessage%" or cu.email_customer like "%noreply%" or cu.email_customer = 'quarantine@ess.barracudanetworks.com' or cu.email_customer like "%no-reply%" or cu.email_customer like "%do_not_reply%" or cu.email_customer = 'postmaster@outlook.com' or cu.email_customer = 'support@gohighlevelassist.freshdesk.com' or cu.email_customer like '%@replies.mastermind.com' or cu.email_customer like "%@deangraziosi.com" or cu.email_customer like "%@mastermind.com" then 1 else 0 end as is_notification
from `bbg-platform.helpscout.conversation_history` c
left join thread bc
  on c.id = bc.conversation_id
left join thread bc1
  on c.id = bc1.conversation_id
  and bc1.message_num_customer = 1
--   and bc.message_num_customer = 1
-- left join thread bu
--   on c.id = bu.conversation_id
--   and bu.message_num_user = 1
left join mailbox m
  on c.folder_id = m.id_folder
-- first customer message & user response
left join threads th
  on c.id = th.conversation_id
  and customer_thread_num = 1
-- all message threads
left join threads th2
  on c.id = th2.conversation_id
-- last customer message & user response
left join threads th3
  on c.id = th3.conversation_id
  and th3.customer_thread_recency = 1
left join user u
  on th3.assigned_to_id = u.id_user
left join customer cu
  on c.primary_customer_id = cu.id_customer
left join cancels x
  on c.id = x.id
where true
-- and date(c.created_at) >= date('2022-01-01')
-- and source_via = 'customer'

 -- and c.number = 408274
 -- and c.number = 1262526
--  and source_via = 'customer'
-- and name_mailbox = 'DG/ Mastermind.com Customer Support'
-- and name_folder != 'Notifications'
 -- and name_folder = 'Spam'
-- and th.user_created_at is not null
 group by all
qualify row_number() over(partition by number order by updated_at desc) = 1