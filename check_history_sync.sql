-- Check if history sync table has any data
SELECT 
    COUNT(*) as total_messages,
    MIN(timestamp) as earliest_timestamp,
    MAX(timestamp) as latest_timestamp,
    MIN(to_timestamp(timestamp)) as earliest_date,
    MAX(to_timestamp(timestamp)) as latest_date
FROM whatsapp_history_sync_message;

-- Check specific date range
SELECT 
    COUNT(*) as messages_in_range
FROM whatsapp_history_sync_message
WHERE timestamp >= EXTRACT(EPOCH FROM TIMESTAMP '2025-11-21 19:13:34')
  AND timestamp <= EXTRACT(EPOCH FROM TIMESTAMP '2025-12-17 17:31:55');

-- Check what chats have history sync data
SELECT 
    chat_jid,
    COUNT(*) as message_count,
    MIN(to_timestamp(timestamp)) as earliest,
    MAX(to_timestamp(timestamp)) as latest
FROM whatsapp_history_sync_message
GROUP BY chat_jid
ORDER BY message_count DESC
LIMIT 20;

-- Check if the message table has media messages in the date range
SELECT 
    COUNT(*) as total_messages,
    COUNT(CASE WHEN metadata::text LIKE '%media%' THEN 1 END) as messages_with_media_metadata,
    COUNT(CASE WHEN metadata::text LIKE '%direct_media%' THEN 1 END) as direct_media_messages,
    COUNT(CASE WHEN metadata::text LIKE '%failed_media%' THEN 1 END) as failed_media_messages
FROM message
WHERE timestamp >= EXTRACT(EPOCH FROM TIMESTAMP '2025-11-21 19:13:34') * 1000000000
  AND timestamp <= EXTRACT(EPOCH FROM TIMESTAMP '2025-12-17 17:31:55') * 1000000000;

