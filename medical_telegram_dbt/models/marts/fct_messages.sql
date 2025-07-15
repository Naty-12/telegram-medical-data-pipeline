-- medical_telegram_dwh/models/marts/fct_messages.sql
{{ config(materialized='table', schema='marts') }}

WITH message_image_counts AS (
    SELECT
        message_id,
        COUNT(image_path) AS num_images_attached
    FROM
        {{ ref('stg_telegram_images') }}
    GROUP BY
        message_id
)
SELECT
    sm.message_id,
    -- FIX: Cast -1 to TEXT to match channel_sk type
    COALESCE(dc.channel_sk, '-1') AS channel_fk, -- Changed from -1 to '-1'
    dd.date_pk AS date_fk,
    sm.sender_id,
    sm.message_text,
    sm.message_views,
    sm.attached_media_type,
    sm.attached_media_path,
    sm.message_length,
    sm.has_media_attachment,
    sm.has_image_attachment,
    sm.message_timestamp AS full_message_timestamp,
    COALESCE(mic.num_images_attached, 0) AS num_images_attached,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM
    {{ ref('stg_telegram_messages') }} sm
LEFT JOIN
    {{ ref('dim_channels') }} dc ON sm.channel_username = dc.channel_username
LEFT JOIN
    {{ ref('dim_dates') }} dd ON sm.message_timestamp::date = dd.date_pk
LEFT JOIN
    message_image_counts mic ON sm.message_id = mic.message_id