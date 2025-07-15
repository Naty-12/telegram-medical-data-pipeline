-- medical_telegram_dwh/models/marts/dim_dates.sql
{{ config(materialized='table', schema='marts') }}

WITH date_spine AS (
    SELECT generate_series(
        -- Set the start date to be on or before your earliest message timestamp
        '2021-10-01'::date, -- A bit before 2021-10-21 to be safe
        -- Set the end date to be on or after your latest message timestamp, plus a buffer for future data
        '2025-12-31'::date, -- Ensures coverage up to end of 2025, providing a future buffer
        '1 day'::interval
    )::date AS date_day
)
SELECT
    date_day AS date_pk,
    TO_CHAR(date_day, 'YYYYMMDD')::INT AS date_id,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Mon') AS month_name_short,
    TO_CHAR(date_day, 'Month') AS month_name_full,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(DOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Dy') AS day_name_short,
    TO_CHAR(date_day, 'Day') AS day_name_full,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    CONCAT(EXTRACT(YEAR FROM date_day), '-Q', EXTRACT(QUARTER FROM date_day)) AS quarter_name,
    CASE
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    TO_CHAR(date_day, 'YYYY-MM') AS year_month
FROM
    date_spine