-- Debug query to check media messages in the database
-- Replace 'YOUR_BRIDGE_ID' and 'YOUR_USER_LOGIN_ID' with actual values

-- 1. Check all messages from the past month
SELECT 
    bridge_id,
    id,
    room_id,
    sender_id,
    timestamp,
    LENGTH(metadata::text) as metadata_length,
    metadata::text LIKE '%direct_media_meta%' as has_direct_media,
    metadata::text LIKE '%media_meta%' as has_media_meta,
    metadata::text LIKE '%failed_media_meta%' as has_failed_media
FROM message
WHERE timestamp >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 month') * 1000
ORDER BY timestamp DESC
LIMIT 50;

-- 2. Check messages for specific portal (replace with actual portal ID)
SELECT 
    bridge_id,
    id,
    room_id,
    sender_id,
    timestamp,
    metadata
FROM message
WHERE room_id = '18166652985@s.whatsapp.net'
  AND timestamp >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 month') * 1000
ORDER BY timestamp DESC
LIMIT 10;

-- 3. Check what portal IDs exist in the database
SELECT DISTINCT room_id, COUNT(*) as message_count
FROM message
WHERE timestamp >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 month') * 1000
GROUP BY room_id
ORDER BY message_count DESC
LIMIT 20;

-- 4. Check if there are any messages with media metadata at all
SELECT COUNT(*) as total_media_messages
FROM message
WHERE (
    metadata::text LIKE '%direct_media_meta%'
    OR metadata::text LIKE '%media_meta%'
    OR metadata::text LIKE '%failed_media_meta%'
);

