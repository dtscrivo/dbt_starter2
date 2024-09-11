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
  , first_name as name
  from `bbg-platform.helpscout.user_history`
 -- where id = 802904
qualify row_number() over(partition by id order by updated_at desc) = 1
)


SELECT 
    t.conversation_id as id_conversation
  , t.id as id_thread
  , action_type as type_user
  , coalesce(u2.name, "Not Assigned") as assigned_to
  , t.assigned_to_id as id_assigned
  , body as message
  , DATETIME(t.created_at, 'America/Phoenix') as date_thread
  , FORMAT_DATETIME('%I%p', DATETIME(t.created_at, 'America/Phoenix')) AS hour_thread
  , coalesce(cu.email_customer, u.email_user, u.name) as thread_created_by
  , t.status as status_thread
  , t.type as type_thread
  , DATETIME(h.rating_created_at, 'America/Phoenix') as date_rating
  , h.rating_comment
  , rating_id as id_rating
  , case when rating_id = 1 then "Great"
         when rating_id = 2 then "Okay"
         when rating_id = 3 then "Not Good"
         else "Not Rated" end as rating
  , rank() over(partition by t.conversation_id order by t.created_at desc) as recency
  , rank() over(partition by t.conversation_id order by t.created_at asc) as message_number
  , rank() over(partition by t.conversation_id, t.created_by_type order by t.created_at desc) recency_by_type
  , rank() over(partition by t.conversation_id, t.created_by_type order by t.created_at asc) message_number_by_type
  , created_by_type
  , c.number as convo_number
FROM `bbg-platform.helpscout.conversation_thread_history` t
LEFT JOIN `bbg-platform.helpscout.happiness_rating` h
  on t.id = h.thread_id
LEFT JOIN user u
  on t.created_by_customer_id = u.id_user
LEFT JOIN user u2
  on t.assigned_to_id = u2.id_user
LEFT JOIN customer cu
  on t.created_by_customer_id = cu.id_customer
LEFT JOIN `bbg-platform.helpscout.conversation_history` c
  on t.conversation_id = c.id


where true
  and t.type IN ('message', 'customer')