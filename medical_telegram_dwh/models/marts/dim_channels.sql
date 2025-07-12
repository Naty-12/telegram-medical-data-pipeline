-- medical_telegram_dwh/models/marts/dim_channels.sql
{{ config(materialized='table', schema='marts') }}

WITH unique_channels AS (
    SELECT
        DISTINCT channel_username
    FROM
        {{ ref('stg_telegram_messages') }} -- Use messages as the source for channels
    WHERE
        channel_username IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['channel_username']) }} AS channel_sk, -- Surrogate key
    channel_username
    -- Add more channel-specific attributes here if you can extract them or enrich them later
FROM
    unique_channels