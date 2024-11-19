{% snapshot snap_lead_details %}
{{
   config(
        target_schema=generate_schema_name('snapshots'),
        unique_key='id_contact',
        strategy='check',
        check_cols=['email','outbound_lead_source','lead_status','id_setter','id_closer','id_owner','most_recent_gg_convo','lead_source_date','lead_status_date','setter_lead_assigned_date','setter_assigned_date','closer_assigned_date','owner_assigned_date']
   )
}}
SELECT
    id as id_contact,
    current_date() as dt,
    analytics.fnEmail(property_email) as email,
    property_outbound_lead_source as outbound_lead_source,
    property_hs_lead_status as lead_status,
    property_setter_contact_owner as id_setter,
    property_closer_contact_owner as id_closer,
    property_hubspot_owner_id as id_owner,
    property_most_recent_gg_convo as most_recent_gg_convo,
    DATETIME(property_most_recent_date_assigned_outbound_lead_source, 'America/Phoenix') as lead_source_date, 
    DATETIME(property_lead_status_last_updated, 'America/Phoenix') as lead_status_date,
    DATETIME(property_setter_lead_assigned_date, 'America/Phoenix') as setter_lead_assigned_date,
    DATETIME(property_setter_contact_owner_assigned_date, 'America/Phoenix') as setter_assigned_date,
    DATETIME(property_closer_contact_owner_assigned_date, 'America/Phoenix') as closer_assigned_date,
    DATETIME(property_hubspot_owner_assigneddate, 'America/Phoenix') as owner_assigned_date
FROM {{ ref('bbg-platform.hubspot2.contact') }}  -- Source table to pull device data from
{% endsnapshot %}