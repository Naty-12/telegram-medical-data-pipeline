{{ config(materialized='table', schema='marts') }}

WITH latest_detections AS (
    SELECT
        message_id,
        image_path,
        detected_object_class,
        confidence_score,
        detection_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY image_path, detected_object_class 
            ORDER BY detection_timestamp DESC
        ) as rn
    FROM
        {{ source('public_staging', 'image_detections_staging') }}
)

SELECT
    -- {{ dbt_utils.generate_surrogate_key(['message_id', 'image_path', 'detected_object_class']) }} AS image_detection_sk,
    ld.message_id,
    fm.channel_fk,
    fm.date_fk,
    ld.image_path,
    ld.detected_object_class,
    ld.confidence_score,
    ld.detection_timestamp,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM
    latest_detections ld
INNER JOIN  -- change here from LEFT JOIN to INNER JOIN
    {{ ref('fct_messages') }} fm ON ld.message_id = fm.message_id
WHERE
    ld.rn = 1