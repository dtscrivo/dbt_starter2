{{ config(materialized='table') }}

WITH events AS (
    SELECT 
        analytics.fnEmail(property_email) as email,
        -- PREVIEW EVENTS
        -- vpe = virtual
        -- ippe = in-person
        MAX(case when name = "Registered - Mastermind Preview Event In-Person (Phoenix 8.21.24-8.25.24)" then added_at else null end) as reg_ippe_phoenix_8212024,
        MAX(case when name = "Attended - Mastermind Preview Event In-Person (Phoenix 8.21.24-8.25.24)" then added_at else null end) as att_ippe_phoenix_8212024,
        MAX(case when name = "Attended - Mastermind Preview Event In-Person (Charlotte 7.31.24-8.5.24)" then added_at else null end) as att_ippe_charlotte_7312024,
        MAX(case when name = "Registered - Mastermind Preview Event In-Person (Charlotte 7.31.24-8.5.24)" then added_at else null end) as reg_ippe_charlotte_7312024,
        MAX(case when name = "Attended - Mastermind Preview Event (Dallas 2.24.24)" then added_at else null end) as att_vpe_dallas_2242024,
        MAX(case when name = "Registered - Mastermind Preview Event (Washington DC 3.27.24)" then added_at else null end) as reg_vpe_dc_3272024,
        MAX(case when name = "Registered - Mastermind Preview Event (Philadelphia 3.27.24)" then added_at else null end) as reg_vpe_philadelphia_3272024,
        MAX(case when name = "Attended - Mastermind Preview Event (Dallas 2.22.24)" then added_at else null end) as att_vpe_dallas_2222024,
        MAX(case when name = "Attended - Mastermind Preview Event (Philadelphia 3.27.24)" then added_at else null end) as att_vpe_philadelphia_3272024,
        MAX(case when name = "Registered - Mastermind Preview Event (Atlanta 3.14.24)" then added_at else null end) as reg_vpe_atlanta_3142024,
        MAX(case when name = "Attended - Mastermind Preview Event (Atlanta 3.14.24)" then added_at else null end) as att_vpe_atlanta_3142024,
        MAX(case when name = "Attended - Mastermind Preview Event (Washington DC 3.27.24)" then added_at else null end) as att_vpe_dc_3272024,
        MAX(case when name = "Attended - Mastermind Preview Event (San Antonio 2.8.24)" then added_at else null end) as att_vpe_sanantonio_2082024,
        MAX(case when name = "Registered - Mastermind Preview Event (Dallas 2.24.24)" then added_at else null end) as reg_vpe_dallas_2242024,
        MAX(case when name = "Registered - Mastermind Preview Event (Dallas 2.22.24)" then added_at else null end) as reg_vpe_dallas_2222024,
        MAX(case when name = "Registered - Mastermind Preview Event (San Antonio 2.8.24)" then added_at else null end) as reg_vpe_sanantonio_2082024,
        MAX(case when name = "Attended - Mastermind Preview Event" then added_at else null end) as att_vpe,
        MAX(case when name = "Registered - Mastermind Preview Event" then added_at else null end) as reg_vpe,
        MAX(case when name = "Attended - Mastermind Preview Event In-Person" then added_at else null end) as att_ippe,
        MAX(case when name = "Attended - Mastermind Preview Event In-Person (Tampa 4.10.24-4.14.24)" then added_at else null end) as att_ippe_tampa_4102024,
        MAX(case when name = "Registered - Mastermind Preview Event In-Person (Tampa 4.10.24-4.14.24)" then added_at else null end) as reg_ippe_tampa_4102024,
        MAX(case when name = "Registered - Mastermind Preview Event In-Person" then added_at else null end) as reg_inpe,
        MAX(case when name = "Attended - Mastermind Preview Event (No City 3.27.24)" then added_at else null end) as att_vpe__nocity_3272024,
        MAX(case when name = "Registered - Mastermind Preview Event In-Person (Baltimore 7.14.24-7.16.24)" then added_at else null end) as reg_ippe_baltimore_7142024,
        MAX(case when name = "Attended - Mastermind Preview Event In-Person (Baltimore 7.14.24-7.16.24)" then added_at else null end) as att_ippe_baltimore_7142024,
        MAX(case when name = "Attended - Mastermind Preview Event In-Person (Washington DC 7.10.24-7.13.24)" then added_at else null end) as att_ippe_dc_7102024,
        MAX(case when name = "Registered - Mastermind Preview Event In-Person (Washington DC 7.10.24-7.13.24)" then added_at else null end) as reg_ippe_dc_7102024,

        -- Launch
        MAX(case when title = 'RSVP - 2024 World Summit' then added_at else null end) as rsvp_world_summit_8032024,

        -- Program
        MAX(case when name = "Program - Mastermind Business Academy (8.29.24 In-Person)" then added_at else null end) as att_ip_mba_8292024,
        MAX(case when name = "Program - Mastermind Business Academy (6.27.24 In-Person)" then added_at else null end) as att_ip_mba_6272024,
        MAX(case when name LIKE "%Preview Event%" and name LIKE "%In-Person%" and name like "Attended%" then added_at else null end) as IPPE,
        MAX(case when name LIKE "%Preview Event%" and name NOT LIKE "%In-Person%" and name like "Attended%" then added_at else null end) as VPE,
        
        CASE WHEN name LIKE "Attended%" THEN REGEXP_EXTRACT(name, r'\(([^0-9]*)') ELSE NULL END AS city_attended,
        CASE WHEN name LIKE "Registered%" THEN REGEXP_EXTRACT(name, r'\(([^0-9]*)') ELSE NULL END AS city_registered,
        CASE WHEN name LIKE "Attended%" THEN REGEXP_EXTRACT(name, r'\([^\)]*([0-9].*)\)') ELSE NULL END AS date_event
    FROM `bbg-platform.hubspot2.contact` c
    LEFT JOIN `bbg-platform.hubspot2.contact_list_member` m
      on c.id = m.contact_id
    LEFT JOIN `bbg-platform.hubspot2.contact_list` l
      on m.contact_list_id = l.id
    LEFT JOIN `bbg-platform.hubspot2.contact_form_submission` s
      on c.id = s.contact_id


    GROUP BY 
        property_email, name
)
SELECT *
FROM events