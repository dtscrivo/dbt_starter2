{{ config(materialized='table') }}

CREATE TABLE `your_project.your_dataset.date_table` AS
WITH date_range AS (
  SELECT
    DATE_ADD(DATE('2000-01-01'), INTERVAL x DAY) AS date
  FROM
    UNNEST(GENERATE_ARRAY(0, 36525)) AS x  -- Generates dates for 100 years (36525 days)
)
SELECT
  date,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(DAY FROM date) AS day,
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
  EXTRACT(DAYOFYEAR FROM date) AS day_of_year,
  EXTRACT(WEEK FROM date) AS week_of_year,
  EXTRACT(QUARTER FROM date) AS quarter,
  FORMAT_DATE('%A', date) AS day_name,
  FORMAT_DATE('%B', date) AS month_name,
  ISOWEEK(date) AS iso_week,
  DATE_ADD(date, INTERVAL (6 - EXTRACT(DAYOFWEEK FROM date)) % 7 DAY) AS week_ending_friday, -- Corrected Week Ending Friday
  CASE
    WHEN FORMAT_DATE('%A', date) IN ('Saturday', 'Sunday') THEN TRUE
    ELSE FALSE
  END AS is_weekend,
  CASE
    WHEN EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE())
         AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN TRUE
    ELSE FALSE
  END AS is_current_month,
  CASE
    WHEN DATE_TRUNC(DATE_SUB(date, INTERVAL 1 DAY), WEEK) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), WEEK) THEN TRUE
    ELSE FALSE
  END AS is_current_week,
  CASE
    WHEN EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN TRUE
    ELSE FALSE
  END AS is_current_year
FROM
  date_range