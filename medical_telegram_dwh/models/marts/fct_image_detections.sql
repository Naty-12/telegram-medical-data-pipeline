-- medical_telegram_dwh/models/marts/fct_image_detections.sql
{{ config(materialized='table', schema='marts') }}

WITH latest_detections AS (
    -- Select the latest detection for each unique image_path and detected_object_class
    -- In case the script somehow inserts duplicates or you want to track changes
    SELECT
        message_id,
        image_path,
        detected_object_class,
        confidence_score,
        detection_timestamp,
        ROW_NUMBER() OVER (PARTITION BY image_path, detected_object_class ORDER BY detection_timestamp DESC) as rn
    FROM
        {{ source('public_staging', 'image_detections_staging') }} -- Using the public_staging schema
)
SELECT
    -- Generate a surrogate key for this fact table if you need a primary key
    -- {{ dbt_utils.generate_surrogate_key(['message_id', 'image_path', 'detected_object_class']) }} AS image_detection_sk,
    
    ld.message_id,
    fm.channel_fk, -- Link to dim_channels via fct_messages
    fm.date_fk,    -- Link to dim_dates via fct_messages
    ld.image_path,
    ld.detected_object_class,
    ld.confidence_score,
    ld.detection_timestamp,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM
    latest_detections ld
LEFT JOIN
    {{ ref('fct_messages') }} fm ON ld.message_id = fm.message_id
WHERE
    ld.rn = 1 -- Select only the latest detection for each combination