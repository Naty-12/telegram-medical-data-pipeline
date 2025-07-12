-- medical_telegram_dwh/models/staging/stg_telegram_images.sql
{{ config(materialized='view', schema='staging') }}

SELECT
    message_id,
    image_path,
    channel_username,
    image_date
    -- Add any other light transformations or column renaming here if needed
FROM
    {{ source('raw', 'raw_telegram_images') }}
WHERE
    image_path IS NOT NULL