{{ config(materialized='table') }}

WITH events AS (
    SELECT 
        analytics.fnEmail(property_email) as email,
        MAX(case when name like "%Phoenix%" and name like "Attended%" then 1 else 0 end) as phoenix_attended,
        MAX(case when name like "%Phoenix%" and name like "Registered%" then 1 else 0 end) as phoenix_registered,
        MAX(case when name like "%DC%" and name like "Attended%" then 1 else 0 end) as dc_attended,
        MAX(case when name like "%DC%" and name like "Registered%" then 1 else 0 end) as dc_registered,
        MAX(case when name like "%Atlanta%" and name like "Attended%" then 1 else 0 end) as atlanta_attended,
        MAX(case when name like "%Atlanta%" and name like "Registered%" then 1 else 0 end) as atlanta_registered,
        MAX(case when name like "%Philadelphia%" and name like "Attended%" then 1 else 0 end) as philadelphia_attended,
        MAX(case when name like "%Philadelphia%" and name like "Registered%" then 1 else 0 end) as philadelphia_registered,
        MAX(case when name like "%Dallas%" and name like "Attended%" then 1 else 0 end) as dallas_attended,
        MAX(case when name like "%Dallas%" and name like "Registered%" then 1 else 0 end) as dallas_registered,
        MAX(case when name like "%San Antonio%" and name like "Attended%" then 1 else 0 end) as san_antonio_attended,
        MAX(case when name like "%San Antonio%" and name like "Registered%" then 1 else 0 end) as san_antonio_registered,
        MAX(case when name like "%Baltimore%" and name like "Attended%" then 1 else 0 end) as baltimore_attended,
        MAX(case when name like "%Baltimore%" and name like "Registered%" then 1 else 0 end) as baltimore_registered,
        MAX(case when name like "%Tampa%" and name like "Attended%" then 1 else 0 end) as tampa_attended,
        MAX(case when name like "%Tampa%" and name like "Registered%" then 1 else 0 end) as tampa_registered,
        MAX(case when name like "%Charlotte%" and name like "Attended%" then 1 else 0 end) as charlotte_attended,
        MAX(case when name like "%Charlotte%" and name like "Registered%" then 1 else 0 end) as charlotte_registered,
        MAX(case when name like "%Raleigh%" and name like "Attended%" then 1 else 0 end) as raleigh_attended,
        MAX(case when name like "%Raleigh%" and name like "Registered%" then 1 else 0 end) as raleigh_registered,
        MAX(case when name like "%Greensboro%" and name like "Attended%" then 1 else 0 end) as greensboro_attended,
        MAX(case when name like "%Greensboro%" and name like "Registered%" then 1 else 0 end) as greensboro_registered,
        MAX(case when name like "%Greensboro%" and name like "Invited%" then 1 else 0 end) as greensboro_invited,
        MAX(case when name like "%Fayetteville%" and name like "Attended%" then 1 else 0 end) as fayetteville_attended,
        MAX(case when name like "%Fayetteville%" and name like "Registered%" then 1 else 0 end) as fayetteville_registered,
        MAX(case when name like "%Fayetteville%" and name like "Invited%" then 1 else 0 end) as fayetteville_invited,
        MAX(case when name like "%In-Person%" and name like "Attended%" then 1 else 0 end) as inperson_attended,
        MAX(case when name like "%In-Person%" and name like "Registered%" then 1 else 0 end) as inperson_registered,
        MAX(case when title = 'RSVP - 2024 World Summit' then 1 else 0 end) as world_summit_24_registered
    FROM `bbg-platform.hubspot2.contact` c
    LEFT JOIN `bbg-platform.hubspot2.contact_list_member` m
      on c.id = m.contact_id
    LEFT JOIN `bbg-platform.hubspot2.contact_list` l
      on m.contact_list_id = l.id
    LEFT JOIN `bbg-platform.hubspot2.contact_form_submission` s
      on c.id = s.contact_id


    GROUP BY 
        property_email
)
SELECT *
FROM events