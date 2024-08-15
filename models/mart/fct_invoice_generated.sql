{{ config(materialized='table') }}

with initial_payments AS (
    SELECT
        analytics.fnEmail(email) as email,
        id_customer,
        id_price,
        date(date_pi_created) AS date_invoice,
        case when id_price IN ('MBA_pif_inpersonpackage_5997', 'bf22') then 1 else SAFE_CAST(plan_type AS INT64) end AS plan_type -- Using SAFE_CAST to avoid errors
        , amount_collected
        , name_product
    FROM
        `dbt_tscrivo.fct_deal_payments`
    WHERE
        num_payment = 1
        AND SAFE_CAST(plan_type AS INT64) IS NOT NULL -- Ensuring only valid integer values
        AND name_product not like "%@%"
)






, generated_payments AS (
    SELECT
        email,
        id_customer,
        id_price,
        date_invoice,
        plan_type,
        -- Generate each payment date using the SEQUENCE function
        DATE_ADD(date_invoice, INTERVAL n MONTH) AS payment_date,
        n + 1 AS payment_number
        , amount_collected
        , name_product
    FROM
        initial_payments,
        UNNEST(GENERATE_ARRAY(0, plan_type - 1)) AS n
)
SELECT
    email,
    id_customer,
    id_price,
    payment_date,
    payment_number
    , "generated" as status_invoice
    , amount_collected
    , name_product
    , 1 as is_generated
FROM
    generated_payments
 --   where email = 'adiyb@adiybmuhammad.com'
-- where id_price = 'taa_pp_lp_935_7'

ORDER BY
    email,
    id_price,
    payment_date



