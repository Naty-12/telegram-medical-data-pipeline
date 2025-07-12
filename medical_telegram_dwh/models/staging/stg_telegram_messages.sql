-- medical_telegram_dwh/models/staging/stg_telegram_messages.sql
{{ config(materialized='view', schema='staging') }}

SELECT
    message_id,
    message_timestamp,
    channel_username,
    -- Extract specific fields from the raw_json column (adjust based on your actual JSON structure)
    raw_json->>'sender_id' AS sender_id, -- Assuming 'sender_id' is a key in your raw_json
    raw_json->>'message' AS message_text,
    -- FIX: Use COALESCE to default NULL views to 0
    COALESCE((raw_json->>'views')::INTEGER, 0) AS message_views, 
    raw_json->>'media_type' AS attached_media_type,
    raw_json->>'media_path' AS attached_media_path,
    
    -- Derived attributes
    LENGTH(raw_json->>'message') AS message_length,
    CASE WHEN raw_json->>'media_type' IS NOT NULL THEN TRUE ELSE FALSE END AS has_media_attachment,
    CASE WHEN (raw_json->>'media_type') = 'photo' THEN TRUE ELSE FALSE END AS has_image_attachment
FROM
    {{ source('raw', 'raw_telegram_messages') }}
WHERE
    (raw_json->>'message' IS NOT NULL AND TRIM(raw_json->>'message') != '')
    OR (raw_json->>'media_type' IS NOT NULL) -- Include messages with just media, even if no text