
{{ config(materialized='table') }}

with page_views as (
  select distinct
    case when split(url, '.com')[0] = 'https://app.mastermind' then user_id else location end as user_id
    , cast(sent_at as date) as pageview_dte
  from `mastermind-app-analytics.events.pages`
  where split(url, '.com')[0] in ('https://app.mastermind', 'https://thehub.mastermind')
)

select distinct `bbg-platform.analytics.fnEmail`(coalesce(u.email, tu.email)) as email, p.pageview_dte
from page_views p
  left join `core-shard-286816.mysql_mastermind_com.users` u
    on p.user_id = cast(u.id as string)
  left join `core-shard-286816.mysql_mastermind_com.teams` t
    on p.user_id = t.ghl_id
  left join `core-shard-286816.mysql_mastermind_com.users` tu
    on t.owner_id = tu.id
where coalesce(u.email, tu.email) is not null