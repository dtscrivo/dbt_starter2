{{ config(materialized='table') }}


-- Latest Customer Update
with customer as (
  select h.id as id_customer
  , DATETIME(updated_at, 'America/Phoenix') as date_customer_updated
  , DATETIME(created_at, 'America/Phoenix') as date_customer_created
  -- , value as email_customer
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

, last_thread AS (
  select conversation_id
     , assigned_to_id
    , case when assigned_to_id = 1 then "Unassigned" 
        --  when t.type = "lineitem" then "lineitem"
         else coalesce(u2.team, "Not Assigned") end as team
  FROM `helpscout.conversation_thread_history`
  LEFT JOIN user u2
  on assigned_to_id = u2.id_user
  qualify row_number() over(partition by conversation_id order by created_at desc) = 1
)

SELECT 
    t.conversation_id as id_conversation
  , t.id as id_thread
  , t.assigned_to_id as id_assigned
  , t.created_by_customer_id AS id_created_by
  , case when t.assigned_to_id = 1 then "Unassigned" 
        --  when t.type = "lineitem" then "lineitem"
         else coalesce(u2.name, "Not Assigned") end as assigned_to
  , case when t.assigned_to_id = 1 then "Unassigned" 
        --  when t.type = "lineitem" then "lineitem"
         else coalesce(u2.team, "Not Assigned") end as team_thread

  , action_text
  , case when lower(action_text) like "%assigned%" then "assigned" 
         when lower(action_text) like "%close%" then "closed"
         when lower(action_text) like "%moved%" then "moved"
         when lower(action_text) like "%%triggered%" then "workflow"
         when lower(action_text) like "%merged%" then "merged"
         else action_text end as action
  , case when t.type = "customer" then 1 else 0 end as is_customer
  , case when lower(action_text) like "%close%" OR t.status = "closed" then 1 else 0 end as is_close 
  , DATETIME(t.created_at, 'America/Phoenix') as date_thread
  , FORMAT_DATETIME('%I%p', DATETIME(t.created_at, 'America/Phoenix')) AS hour_thread
  -- , coalesce(cu.email_customer, u.email_user, u.name) as thread_created_by
  , case when t.type = "customer" then "Customer" else coalesce(u.email_user, u.name, "customer") end as created_by
  , case when t.type = "customer" then "Customer" else u.name end as thread_creator
  , t.status as status_thread
  , t.type as type_thread
  , DATETIME(h.rating_created_at, 'America/Phoenix') as date_rating
  , h.rating_comment
  , rating_id as id_rating
  , case when rating_id = 1 then "Great"
         when rating_id = 2 then "Okay"
         when rating_id = 3 then "Not Good"
         else "Not Rated" end as rating
  , dense_rank() over(partition by t.conversation_id order by t.created_at desc) as recency
  , dense_rank() over(partition by t.conversation_id order by t.created_at asc) as message_number
  , dense_rank() over(partition by t.conversation_id, t.created_by_type order by t.created_at desc) recency_by_type
  , dense_rank() over(partition by t.conversation_id, t.created_by_type order by t.created_at asc) message_number_by_type
  , created_by_type
  , c.number as convo_number
  , m.name_folder
  , m.name_mailbox
  , c.number
  , l.team_convo
FROM `bbg-platform.helpscout.conversation_thread_history` t
LEFT JOIN `bbg-platform.helpscout.happiness_rating` h
  on t.id = h.thread_id
LEFT JOIN user u
  on t.created_by_customer_id = u.id_user
LEFT JOIN user u2
  on t.assigned_to_id = u2.id_user
-- LEFT JOIN customer cu
--   on t.created_by_customer_id = cu.id_customer
LEFT JOIN `bbg-platform.helpscout.conversation_history` c
  on t.conversation_id = c.id
LEFT JOIN mailbox m
  on c.folder_id = m.id_folder
LEFT JOIN last_thread l
  on t.conversation_id = l.conversation_id


where true
 -- and t.type IN ('message', 'customer', 'lineitem')
 -- and DATE(t.created_at) >= DATE('2023-01-01')
 -- and c.number = 1266873
--  and u2.team is not null
 -- and  t.conversation_id =2706087172
  -- and case when lower(action_text) like "%close%" then 1 else 0 end = 1
qualify row_number() over(partition by t.conversation_id, t.id) = 1
ORDER BY t.created_at desc, t.conversation_id