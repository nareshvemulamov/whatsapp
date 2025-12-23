-- Check if there are any media keys stored in the backfill request table
SELECT 
    bridge_id,
    user_login_id,
    message_id,
    portal_id,
    portal_receiver,
    LENGTH(media_key) as media_key_length,
    status,
    error
FROM whatsapp_media_backfill_request
WHERE bridge_id = 'YOUR_BRIDGE_ID'
  AND user_login_id = 'YOUR_USER_LOGIN_ID'
ORDER BY message_id
LIMIT 50;

