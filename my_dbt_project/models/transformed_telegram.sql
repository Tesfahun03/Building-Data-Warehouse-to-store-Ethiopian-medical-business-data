WITH cleaned_data AS (
    SELECT 
        id, 
        channel_title, 
        channel_username, 
        message_id, 
        message, 
        message_date::TIMESTAMP AS message_timestamp, 
        media_path,
        -- Extract Price from message text
        REGEXP_MATCHES(message, '(\d+)\s*birr') AS extracted_price
    FROM {{ ref('staging_telegram') }}
)
SELECT 
    id, 
    channel_title, 
    channel_username, 
    message_id, 
    message, 
    message_timestamp, 
    media_path, 
    extracted_price[1]::INTEGER AS price
FROM cleaned_data;
