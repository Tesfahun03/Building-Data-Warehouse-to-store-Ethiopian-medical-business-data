WITH telegram_data AS (
    SELECT 
        id, 
        channel_title, 
        channel_username, 
        message_id, 
        message, 
        message_date, 
        media_path
    FROM {{ source('public', 'telegram_messages') }}
)
SELECT * FROM telegram_data;
