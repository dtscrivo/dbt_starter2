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
 , user AS (
    SELECT id AS id_user,
           email AS email_user,
           timezone AS user_timezone,
           case when id = 719539 then 'AI' else first_name end AS name,
           team_id,
             case when t.user_id = 793820 then 'Account Changes'
when t.user_id = 789132 then 'Business Hub'
when t.user_id = 741056 then 'Dev Requests'
when t.user_id = 792871 then 'Enterprise'
when t.user_id = 796834 then 'GG'
when t.user_id = 798395 then 'KBB Workshops'
when t.user_id = 797971 then 'Legacy Builder'
when t.user_id = 801929 then 'MBA Students'
when t.user_id = 796896 then 'MM Login Help'
when t.user_id = 796996 then 'Notifications'
when t.user_id = 796690 then 'Obvio Links'
when t.user_id = 799135 then 'Plug N Play'
when t.user_id = 804598 then 'Point Issues'
when t.user_id = 722541 then 'Saves and Declines'
when t.user_id = 719557 then 'Shipping'
when t.user_id = 797096 then 'Tony Leadership Mastery'
when t.user_id = 799141 then 'World Summit'
when t.user_id = 720198 then 'Dev Requests'
when t.user_id = 792032 then 'Unassigned'
when t.user_id = 789553 then 'Unassigned'
when t.user_id = 789553 then 'MBA Students'
when t.user_id = 802907 then 'Saves and Declines'
when t.user_id = 802904 then 'Saves and Declines'
when t.user_id = 802905 then 'Saves and Declines'
when t.user_id = 802903 then 'MBA Students'
when t.user_id = 720197 then 'Saves and Declines'
when t.user_id = 720189 then 'MBA Students'
when t.user_id = 720184 then 'Saves and Declines'
when t.user_id = 800463 then 'Saves and Declines'
when t.user_id = 798573 then 'Saves and Declines'
when t.user_id = 720018 then 'Business Hub'
when t.user_id = 720186 then 'Business Hub'
when t.user_id = 792033 then 'KBB Workshops'
when t.user_id = 800462 then 'Business Hub'
when t.user_id = 800670 then 'Business Hub'
when t.user_id = 800464 then 'Business Hub'
when t.user_id = 722084 then 'Saves and Declines'
when t.user_id = 790317 then 'MBA Students'
when t.user_id = 1193736 then 'Notifications'
             ELSE u.first_name
           END AS team
    FROM `bbg-platform.helpscout.user_history` u
    LEFT JOIN `bbg-platform.helpscout.team_user_history` t
      ON u.id = t.user_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
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

, action as (
SELECT conversation_id
   , u.team
 FROM `bbg-platform.helpscout.conversation_thread_history` t
 LEFT JOIN user u
   on t.assigned_to_id = u.id_user
where true
  and type = "lineitem"
 -- and conversation_id = 2639466830
qualify row_number() over(partition by t.conversation_id order by conversation_updated_at desc) = 1
)

, convos as (
  select *
  from `helpscout.conversation_history`
  qualify row_number() over(partition by id order by updated_at desc) = 1
 )

SELECT 
    t.conversation_id as id_conversation
  , t.id as id_thread
  , t.assigned_to_id as id_assigned
  , t.created_by_customer_id AS id_created_by
  -- , case when t.assigned_to_id = 1 then "Unassigned" 
  --       --  when t.type = "lineitem" then "lineitem"
  --        else coalesce(ua.name, "Not Assigned") end as assigned_to
  -- , case when t.assigned_to_id = 1 then "Unassigned" 
  --       --  when t.type = "lineitem" then "lineitem"
  --        else coalesce(u2.team, "Not Assigned") end as team_thread

  , action_text
  , case when lower(action_text) like "%assigned%" then "assigned" 
         when lower(action_text) like "%close%" then "closed"
         when lower(action_text) like "%moved%" then "moved"
         when lower(action_text) like "%%triggered%" then "workflow"
         when lower(action_text) like "%merged%" then "merged"
         else action_text end as action
  , case when t.type = "customer" then 1 else 0 end as is_customer
  , case when lower(action_text) like "%close%" OR t.status = "closed" OR c.status = 'closed' then 1 else 0 end as is_closed 
  , DATETIME(t.created_at, 'America/Phoenix') as date_thread
  , FORMAT_DATETIME('%I%p', DATETIME(t.created_at, 'America/Phoenix')) AS hour_thread
  -- , coalesce(cu.email_customer, u.email_user, u.name) as thread_created_by
  , t.status as status_thread
  , t.type as type_thread
  , DATETIME(h.rating_created_at, 'America/Phoenix') as date_rating
  , h.rating_comment
  , rating_id as id_rating
  , case when rating_id = 1 then "Great"
         when rating_id = 2 then "Okay"
         when rating_id = 3 then "Not Good"
         else "Not Rated" end as rating
  , case when t.type IN ('message','customer') then dense_rank() over(partition by t.conversation_id order by t.created_at desc) else null end as recency
  , case when t.type IN ('message','customer') then dense_rank() over(partition by t.conversation_id order by t.created_at asc) else null end as message_number
  , dense_rank() over(partition by t.conversation_id, t.created_by_type order by t.created_at desc) recency_by_type
  , dense_rank() over(partition by t.conversation_id, t.created_by_type order by t.created_at asc) message_number_by_type
  , created_by_type
  , c.number as convo_number
  , m.name_folder
  , m.name_mailbox
  , m.mailbox
  , c.number
  , l.team as team_convo
  , c.status as status_convo
  , case when l.team = "Notifications" or cu.email_customer like "%systemmessage%" or cu.email_customer like "%noreply%" or cu.email_customer = 'quarantine@ess.barracudanetworks.com' or cu.email_customer like "%no-reply%" or cu.email_customer like "%do_not_reply%" or cu.email_customer = 'postmaster@outlook.com' or cu.email_customer = 'support@gohighlevelassist.freshdesk.com' or cu.email_customer like '%@replies.mastermind.com' or cu.email_customer like "%@deangraziosi.com" or cu.email_customer like "%@mastermind.com" then 1 else 0 end as is_notification


 , case when t.type = 'customer' then 'customer' 
        when t.type = 'message' and coalesce(u.name,ut.name) is null then 'Not Found' else coalesce(u.name,ut.name) end AS creator
 , case when coalesce(l.team, a.team, u.team, ua.team) is null then 'Not Assigned' else coalesce(l.team, a.team, u.team, ua.team) end AS team
 , case when case when lower(action_text) like "%close%" OR t.status = "closed" OR c.status = 'closed' then 1 else 0 end = 1 and 
        case when t.type IN ('message','customer') then dense_rank() over(partition by t.conversation_id order by t.created_at desc) else null end = 1 and
        case when t.type IN ('message','customer') then dense_rank() over(partition by t.conversation_id order by t.created_at asc) else null end = 1
        then 1 else 0 end as is_closed_unresponded
FROM `bbg-platform.helpscout.conversation_thread_history` t
LEFT JOIN `bbg-platform.helpscout.happiness_rating` h
  on t.id = h.thread_id
LEFT JOIN user u
  on t.created_by_customer_id = u.id_user
LEFT JOIN user ua
  on t.assigned_to_id = ua.id_user
LEFT JOIN user ut
  on t.assigned_to_id = ut.team_id
 LEFT JOIN customer cu
  on t.created_by_customer_id = cu.id_customer
LEFT JOIN convos c
  on t.conversation_id = c.id
LEFT JOIN mailbox m
  on c.folder_id = m.id_folder
LEFT JOIN last_thread l
  on t.conversation_id = l.conversation_id
LEFT JOIN action a
  on t.conversation_id = a.conversation_id

where true
qualify row_number() over(partition by t.conversation_id, t.id) = 1
ORDER BY t.created_at desc, t.conversation_id