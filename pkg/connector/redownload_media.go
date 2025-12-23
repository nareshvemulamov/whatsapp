// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2024 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/wadb"
	"go.mau.fi/mautrix-whatsapp/pkg/msgconv"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// MessageWithPortal wraps a database.Message with its portal key information
type MessageWithPortal struct {
	Message   *database.Message
	PortalKey networkid.PortalKey
}

// GetMediaMessagesFromPastMonths retrieves all messages with media (DirectMediaMeta or FailedMediaMeta)
// from the past N months for a specific user login, optionally filtered by portal/chat
func (wa *WhatsAppClient) GetMediaMessagesFromPastMonths(ctx context.Context, months int, portalID networkid.PortalID) ([]*MessageWithPortal, error) {
	// Calculate the time threshold (N months ago)
	threshold := time.Now().AddDate(0, -months, 0)
	return wa.GetMediaMessagesInDateRange(ctx, threshold, time.Now(), portalID)
}

// GetMediaMessagesInDateRange retrieves all messages with media (DirectMediaMeta or FailedMediaMeta)
// within a specific date range for a specific user login, optionally filtered by portal/chat
func (wa *WhatsAppClient) GetMediaMessagesInDateRange(ctx context.Context, startTime, endTime time.Time, portalID networkid.PortalID) ([]*MessageWithPortal, error) {
	log := zerolog.Ctx(ctx)

	// Database stores timestamps in nanoseconds
	startNs := startTime.UnixNano()
	endNs := endTime.UnixNano()

	log.Info().
		Time("start_time", startTime).
		Time("end_time", endTime).
		Int64("start_ns", startNs).
		Int64("end_ns", endNs).
		Str("portal_id", string(portalID)).
		Msg("Querying media messages from database")

	// Query messages from the bridgev2 message table using direct SQL
	// The message table schema: bridge_id, id, part_id, mxid, room_id, room_receiver, sender_id, timestamp, metadata
	var query string
	var args []interface{}

	if portalID != "" {
		// Filter by specific portal/chat
		query = `
			SELECT bridge_id, id, part_id, mxid, room_id, room_receiver, sender_id, timestamp, metadata
			FROM message
			WHERE bridge_id = $1
			  AND room_id = $2
			  AND timestamp >= $3
			  AND timestamp <= $4
			  AND (
			      metadata::text LIKE '%"direct_media_meta"%'
			      OR metadata::text LIKE '%"media_meta"%'
			  )
			ORDER BY timestamp DESC
		`
		args = []interface{}{wa.Main.Bridge.ID, portalID, startNs, endNs}
	} else {
		// Get all media messages for this user
		query = `
			SELECT bridge_id, id, part_id, mxid, room_id, room_receiver, sender_id, timestamp, metadata
			FROM message
			WHERE bridge_id = $1
			  AND (room_receiver = $2 OR room_receiver = '')
			  AND timestamp >= $3
			  AND timestamp <= $4
			  AND (
			      metadata::text LIKE '%"direct_media_meta"%'
			      OR metadata::text LIKE '%"media_meta"%'
			  )
			ORDER BY timestamp DESC
		`
		args = []interface{}{wa.Main.Bridge.ID, wa.UserLogin.ID, startNs, endNs}
	}

	rows, err := wa.Main.Bridge.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query media messages: %w", err)
	}
	defer rows.Close()

	log.Debug().
		Str("query", query).
		Interface("args", args).
		Msg("Executed query for media messages")

	var messages []*MessageWithPortal
	for rows.Next() {
		msg := &database.Message{}
		var metadataJSON []byte
		var roomID, roomReceiver, senderID string
		var timestampNs int64

		err := rows.Scan(
			&msg.BridgeID,
			&msg.ID,
			&msg.PartID,
			&msg.MXID,
			&roomID,
			&roomReceiver,
			&senderID,
			&timestampNs,
			&metadataJSON,
		)
		if err != nil {
			log.Err(err).Msg("Failed to scan message row")
			continue
		}

		// Convert Unix nanoseconds to time.Time
		msg.Timestamp = time.Unix(0, timestampNs)

		// Parse metadata
		var metadata waid.MessageMetadata
		if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
			log.Err(err).Str("message_id", string(msg.ID)).Msg("Failed to unmarshal message metadata")
			continue
		}

		// Only include messages that actually have media metadata
		if metadata.DirectMediaMeta != nil || metadata.FailedMediaMeta != nil {
			msg.Metadata = &metadata

			log.Debug().
				Str("message_id", string(msg.ID)).
				Time("message_timestamp", msg.Timestamp).
				Int64("message_timestamp_ns", timestampNs).
				Int64("start_ns", startNs).
				Int64("end_ns", endNs).
				Bool("within_range", timestampNs >= startNs && timestampNs <= endNs).
				Msg("Found media message")

			messages = append(messages, &MessageWithPortal{
				Message: msg,
				PortalKey: networkid.PortalKey{
					ID:       networkid.PortalID(roomID),
					Receiver: networkid.UserLoginID(roomReceiver),
				},
			})
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating message rows: %w", err)
	}

	log.Info().Int("count", len(messages)).Msg("Found media messages")
	return messages, nil
}

// RedownloadMediaForMessages downloads media from WhatsApp servers and re-uploads to Matrix
func (wa *WhatsAppClient) RedownloadMediaForMessages(ctx context.Context, messages []*MessageWithPortal) (int, int, error) {
	log := zerolog.Ctx(ctx)

	successCount := 0
	failCount := 0

	for _, msgWithPortal := range messages {
		msg := msgWithPortal.Message
		portalKey := msgWithPortal.PortalKey

		metadata := msg.Metadata.(*waid.MessageMetadata)

		var keys *msgconv.FailedMediaKeys
		var hasMedia bool

		// Extract media keys from DirectMediaMeta or FailedMediaMeta
		if metadata.DirectMediaMeta != nil {
			// DirectMediaMeta contains FailedMediaKeys directly
			keys = &msgconv.FailedMediaKeys{}
			if err := json.Unmarshal(metadata.DirectMediaMeta, keys); err != nil {
				log.Err(err).Str("message_id", string(msg.ID)).Msg("Failed to unmarshal direct media metadata")
				failCount++
				continue
			}
			log.Debug().
				Str("message_id", string(msg.ID)).
				Int("key_length", len(keys.Key)).
				Str("direct_path", keys.DirectPath).
				Msg("Parsed DirectMediaMeta")
			hasMedia = true
		} else if metadata.FailedMediaMeta != nil {
			// FailedMediaMeta contains PreparedMedia, which has FailedMediaKeys in whatsapp_media field
			var preparedMedia msgconv.PreparedMedia
			if err := json.Unmarshal(metadata.FailedMediaMeta, &preparedMedia); err != nil {
				log.Err(err).Str("message_id", string(msg.ID)).Msg("Failed to unmarshal failed media metadata")
				failCount++
				continue
			}
			keys = preparedMedia.FailedKeys
			log.Debug().
				Str("message_id", string(msg.ID)).
				Int("key_length", len(keys.Key)).
				Str("direct_path", keys.DirectPath).
				Msg("Parsed FailedMediaMeta")
			hasMedia = true
		}

		if !hasMedia || len(keys.Key) == 0 {
			if hasMedia {
				log.Warn().
					Str("message_id", string(msg.ID)).
					Bool("has_direct_media", metadata.DirectMediaMeta != nil).
					Bool("has_failed_media", metadata.FailedMediaMeta != nil).
					Msg("Message has media metadata but no encryption key")
			} else {
				log.Debug().Str("message_id", string(msg.ID)).Msg("Message has no media metadata")
			}
			failCount++
			continue
		}

		// Download media from WhatsApp servers and re-upload to Matrix
		err := wa.downloadAndReuploadMedia(ctx, msg, keys, portalKey)
		if err != nil {
			log.Err(err).Str("message_id", string(msg.ID)).Msg("Failed to download and re-upload media")
			failCount++
		} else {
			log.Info().Str("message_id", string(msg.ID)).Msg("Successfully downloaded and re-uploaded media")
			successCount++
		}

		// Small delay to avoid overwhelming WhatsApp servers
		time.Sleep(200 * time.Millisecond)
	}

	return successCount, failCount, nil
}

// downloadAndReuploadMedia downloads media from WhatsApp and re-uploads it to Matrix
func (wa *WhatsAppClient) downloadAndReuploadMedia(ctx context.Context, msg *database.Message, keys *msgconv.FailedMediaKeys, portalKey networkid.PortalKey) error {
	log := zerolog.Ctx(ctx)

	// Get the portal
	portal, portalErr := wa.Main.Bridge.GetPortalByKey(ctx, portalKey)
	if portalErr != nil {
		return fmt.Errorf("failed to get portal: %w", portalErr)
	} else if portal == nil {
		return fmt.Errorf("portal not found")
	}

	// Download media from WhatsApp servers
	log.Debug().
		Str("message_id", string(msg.ID)).
		Str("direct_path", keys.DirectPath).
		Bool("has_direct_path", keys.DirectPath != "").
		Msg("Attempting to download media from WhatsApp servers")

	data, err := wa.Client.Download(ctx, keys)
	if err != nil {
		// Check if download fails with 403/404/410 or no URL present
		// Need to check both errors.Is() and error message because errors can be wrapped
		errStr := err.Error()
		isExpiredMedia := errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
			errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
			errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410) ||
			errors.Is(err, whatsmeow.ErrNoURLPresent) ||
			strings.Contains(errStr, "status code 403") ||
			strings.Contains(errStr, "status code 404") ||
			strings.Contains(errStr, "status code 410") ||
			strings.Contains(errStr, "no url present")

		if isExpiredMedia {
			log.Warn().
				Err(err).
				Str("message_id", string(msg.ID)).
				Msg("Media not available on WhatsApp CDN, requesting from phone (this requires your phone to be online)")

			// Create a context with longer timeout for phone response
			phoneCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()

			// Use requestAndWaitDirectMedia which handles creating the retry state and waiting
			retryErr := wa.requestAndWaitDirectMedia(phoneCtx, msg.ID, keys)
			if retryErr != nil {
				log.Warn().
					Err(retryErr).
					Str("message_id", string(msg.ID)).
					Msg("Phone did not provide media - it may be offline, media may be deleted, or media is too old")
				return fmt.Errorf("media expired on WhatsApp CDN and phone did not respond: %w", retryErr)
			}

			// keys.DirectPath has been updated by requestAndWaitDirectMedia, retry download
			log.Info().
				Str("message_id", string(msg.ID)).
				Str("new_direct_path", keys.DirectPath).
				Msg("Phone provided new media URL, retrying download")
			data, err = wa.Client.Download(ctx, keys)
			if err != nil {
				return fmt.Errorf("failed to download media even after phone provided new URL: %w", err)
			}
		} else {
			return fmt.Errorf("failed to download media from WhatsApp: %w", err)
		}
	}

	// Detect MIME type if not set
	mimeType := string(keys.Type)
	if mimeType == "" {
		mimeType = http.DetectContentType(data)
	}

	log.Debug().
		Str("message_id", string(msg.ID)).
		Int("size", len(data)).
		Str("mime_type", mimeType).
		Msg("Downloaded media, uploading to Matrix")

	// Upload to Matrix
	intent, ok := portal.GetIntentFor(ctx, bridgev2.EventSender{
		SenderLogin: wa.UserLogin.ID,
	}, wa.UserLogin, bridgev2.RemoteEventMessage)
	if !ok {
		return fmt.Errorf("failed to get intent for portal")
	}

	uploadURL, uploadFile, err := intent.UploadMedia(ctx, portal.MXID, data, "", mimeType)
	if err != nil {
		return fmt.Errorf("failed to upload media to Matrix: %w", err)
	}

	log.Info().
		Str("sender", string(msg.SenderID)).
		Str("receiver", string(portalKey.Receiver)).
		Str("portal_id", string(portalKey.ID)).
		Str("mxc_url", string(uploadURL)).
		Str("message_id", string(msg.ID)).
		Time("timestamp", msg.Timestamp).
		Int64("timestamp_epoch", msg.Timestamp.Unix()).
		Str("mime_type", mimeType).
		Int("file_size_bytes", len(data)).
		Msg("Successfully re-uploaded media to Synapse")

	// Note: uploadFile is the encrypted file info (if encryption is enabled)
	_ = uploadFile

	// TODO: Optionally update the message in the database with the new MXC URI
	// This would require editing the Matrix message, which might not be desired

	return nil
}

// HistorySyncMediaMessage represents a media message from history sync
type HistorySyncMediaMessage struct {
	ChatJID   types.JID
	SenderJID types.JID
	MessageID string
	Timestamp time.Time
	MediaMsg  msgconv.MediaMessage
	MsgType   string
	RoomID    id.RoomID
	RoomMXID  id.RoomID
	PortalKey networkid.PortalKey
}

// GetHistorySyncMediaMessagesInDateRange retrieves media messages from whatsapp_history_sync_message table
// This includes ALL media messages (even successfully uploaded ones) because the raw protobuf contains encryption keys
func (wa *WhatsAppClient) GetHistorySyncMediaMessagesInDateRange(ctx context.Context, startTime, endTime time.Time) ([]*HistorySyncMediaMessage, error) {
	log := zerolog.Ctx(ctx)

	startUnix := startTime.Unix()
	endUnix := endTime.Unix()

	query := `
		SELECT hsm.chat_jid, hsm.sender_jid, hsm.message_id, hsm.timestamp, hsm.data, p.mxid
		FROM whatsapp_history_sync_message hsm
		LEFT JOIN portal p ON p.bridge_id = hsm.bridge_id AND p.id = hsm.chat_jid AND p.receiver = hsm.user_login_id
		WHERE hsm.bridge_id = $1
		  AND hsm.user_login_id = $2
		  AND hsm.timestamp >= $3
		  AND hsm.timestamp <= $4
		ORDER BY hsm.timestamp DESC
	`

	rows, err := wa.Main.Bridge.DB.Query(ctx, query, wa.Main.Bridge.ID, wa.UserLogin.ID, startUnix, endUnix)
	if err != nil {
		return nil, fmt.Errorf("failed to query history sync messages: %w", err)
	}
	defer rows.Close()

	var messages []*HistorySyncMediaMessage

	for rows.Next() {
		var chatJIDStr, senderJIDStr, messageID string
		var timestamp int64
		var data []byte
		var roomMXID *id.RoomID

		if err := rows.Scan(&chatJIDStr, &senderJIDStr, &messageID, &timestamp, &data, &roomMXID); err != nil {
			log.Err(err).Msg("Failed to scan history sync message row")
			continue
		}

		// Parse JIDs
		chatJID, err := types.ParseJID(chatJIDStr)
		if err != nil {
			log.Err(err).Str("chat_jid", chatJIDStr).Msg("Failed to parse chat JID")
			continue
		}

		senderJID, err := types.ParseJID(senderJIDStr)
		if err != nil {
			log.Err(err).Str("sender_jid", senderJIDStr).Msg("Failed to parse sender JID")
			continue
		}

		// Unmarshal protobuf
		var historySyncMsg waHistorySync.HistorySyncMsg
		if err := proto.Unmarshal(data, &historySyncMsg); err != nil {
			log.Err(err).Msg("Failed to unmarshal history sync message")
			continue
		}

		webMsg := historySyncMsg.GetMessage()
		if webMsg == nil || webMsg.Message == nil {
			continue
		}

		waMsg := webMsg.Message

		// Check if this is a media message and extract it
		var mediaMsg msgconv.MediaMessage
		var msgType string

		if waMsg.ImageMessage != nil {
			mediaMsg = waMsg.ImageMessage
			msgType = "image"
		} else if waMsg.VideoMessage != nil {
			mediaMsg = waMsg.VideoMessage
			msgType = "video"
		} else if waMsg.AudioMessage != nil {
			mediaMsg = waMsg.AudioMessage
			msgType = "audio"
		} else if waMsg.DocumentMessage != nil {
			mediaMsg = waMsg.DocumentMessage
			msgType = "document"
		} else if waMsg.StickerMessage != nil {
			mediaMsg = waMsg.StickerMessage
			msgType = "sticker"
		} else {
			// Not a media message, skip
			continue
		}

		// Skip if no media key (shouldn't happen, but be safe)
		if len(mediaMsg.GetMediaKey()) == 0 {
			continue
		}

		portalKey := networkid.PortalKey{
			ID:       networkid.PortalID(chatJIDStr),
			Receiver: wa.UserLogin.ID,
		}

		var roomID id.RoomID
		if roomMXID != nil {
			roomID = *roomMXID
		}

		messages = append(messages, &HistorySyncMediaMessage{
			ChatJID:   chatJID,
			SenderJID: senderJID,
			MessageID: messageID,
			Timestamp: time.Unix(timestamp, 0),
			MediaMsg:  mediaMsg,
			MsgType:   msgType,
			RoomID:    roomID,
			RoomMXID:  roomID,
			PortalKey: portalKey,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating history sync messages: %w", err)
	}

	log.Info().Int("count", len(messages)).Msg("Found media messages in history sync")

	return messages, nil
}

// RedownloadHistorySyncMedia downloads and re-uploads media from history sync messages
func (wa *WhatsAppClient) RedownloadHistorySyncMedia(ctx context.Context, messages []*HistorySyncMediaMessage) (successCount, failCount int, err error) {
	log := zerolog.Ctx(ctx)

	for _, msg := range messages {
		log.Info().
			Str("chat_jid", msg.ChatJID.String()).
			Str("sender_jid", msg.SenderJID.String()).
			Str("message_id", msg.MessageID).
			Str("msg_type", msg.MsgType).
			Time("timestamp", msg.Timestamp).
			Msg("Redownloading media from history sync")

		// Download media from WhatsApp
		data, err := wa.Client.Download(ctx, msg.MediaMsg)
		if err != nil {
			// Check for expired media errors
			if errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
				errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
				errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410) ||
				strings.Contains(err.Error(), "status code 403") ||
				strings.Contains(err.Error(), "status code 404") ||
				strings.Contains(err.Error(), "status code 410") {
				log.Warn().
					Err(err).
					Str("message_id", msg.MessageID).
					Msg("Media expired on WhatsApp CDN, cannot redownload")
				failCount++
				continue
			}

			log.Err(err).
				Str("message_id", msg.MessageID).
				Msg("Failed to download media from WhatsApp")
			failCount++
			continue
		}

		// Get portal
		portal, err := wa.Main.Bridge.GetPortalByKey(ctx, msg.PortalKey)
		if err != nil {
			log.Err(err).
				Str("portal_key", msg.PortalKey.String()).
				Msg("Failed to get portal")
			failCount++
			continue
		}

		if portal == nil {
			log.Warn().
				Str("portal_key", msg.PortalKey.String()).
				Msg("Portal not found, skipping")
			failCount++
			continue
		}

		// Get intent for sender
		ghost, err := wa.Main.Bridge.GetGhostByID(ctx, networkid.UserID(msg.SenderJID.String()))
		if err != nil {
			log.Err(err).
				Str("sender_jid", msg.SenderJID.String()).
				Msg("Failed to get ghost for sender")
			failCount++
			continue
		}

		var intent bridgev2.MatrixAPI
		if ghost != nil {
			intent = ghost.Intent
		} else {
			intent = portal.Bridge.Bot
		}

		// Detect MIME type
		mimeType := msg.MediaMsg.GetMimetype()
		if mimeType == "" {
			mimeType = http.DetectContentType(data)
		}

		// Upload to Synapse
		uploadURL, uploadFile, err := intent.UploadMedia(ctx, portal.MXID, data, "", mimeType)
		if err != nil {
			log.Err(err).
				Str("message_id", msg.MessageID).
				Msg("Failed to upload media to Synapse")
			failCount++
			continue
		}

		successCount++

		log.Info().
			Str("sender", msg.SenderJID.String()).
			Str("receiver", string(msg.PortalKey.Receiver)).
			Str("portal_id", string(msg.PortalKey.ID)).
			Str("mxc_url", string(uploadURL)).
			Str("message_id", msg.MessageID).
			Time("timestamp", msg.Timestamp).
			Int64("timestamp_epoch", msg.Timestamp.Unix()).
			Str("mime_type", mimeType).
			Int("file_size_bytes", len(data)).
			Msg("Successfully re-uploaded media from history sync to Synapse")

		_ = uploadFile
	}

	return successCount, failCount, nil
}

// RequestMediaFromBackfillTable requests media using keys stored in whatsapp_media_backfill_request table
// This table contains encryption keys for media that failed to download during backfill
func (wa *WhatsAppClient) RequestMediaFromBackfillTable(ctx context.Context, startTime, endTime time.Time) (successCount, failCount int, err error) {
	log := zerolog.Ctx(ctx)

	// Get all unrequested media from the backfill table
	mediaRequests, err := wa.Main.DB.MediaRequest.GetUnrequestedForUserLogin(ctx, wa.UserLogin.ID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query media backfill requests: %w", err)
	}

	log.Info().
		Int("total_requests", len(mediaRequests)).
		Msg("Found media requests in backfill table")

	if len(mediaRequests) == 0 {
		return 0, 0, fmt.Errorf("no media requests found in backfill table")
	}

	// Process each media request
	for i, req := range mediaRequests {
		log.Info().
			Int("progress", i+1).
			Int("total", len(mediaRequests)).
			Str("message_id", string(req.MessageID)).
			Msg("Processing media request from backfill table")

		// Send media request to phone
		wa.sendMediaRequest(ctx, req)

		if req.Status == wadb.MediaBackfillRequestStatusRequested {
			successCount++
		} else {
			failCount++
		}
	}

	return successCount, failCount, nil
}

// RequestMediaFromPhone requests media from the phone for messages in a date range
// This is used when Synapse media store is lost but messages exist in the database
func (wa *WhatsAppClient) RequestMediaFromPhone(ctx context.Context, startTime, endTime time.Time) (successCount, failCount int, err error) {
	log := zerolog.Ctx(ctx)

	startNs := startTime.UnixNano()
	endNs := endTime.UnixNano()

	// Query all messages in the date range
	// The message table uses room_id and room_receiver, not portal_id
	query := `
		SELECT m.bridge_id, m.id, m.part_id, m.mxid, m.room_id, m.room_receiver, m.sender_id, m.timestamp, m.metadata
		FROM message m
		WHERE m.bridge_id = $1
		  AND m.room_receiver = $2
		  AND m.timestamp >= $3
		  AND m.timestamp <= $4
		ORDER BY m.timestamp ASC
	`

	log.Info().
		Str("bridge_id", string(wa.Main.Bridge.ID)).
		Str("user_login_id", string(wa.UserLogin.ID)).
		Int64("start_ns", startNs).
		Int64("end_ns", endNs).
		Time("start_time", startTime).
		Time("end_time", endTime).
		Str("query", query).
		Msg("Executing query to find messages with media metadata")

	rows, err := wa.Main.Bridge.DB.Query(ctx, query, wa.Main.Bridge.ID, wa.UserLogin.ID, startNs, endNs)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	log.Info().Msg("Query executed successfully, scanning rows...")

	type messageInfo struct {
		BridgeID     networkid.BridgeID
		ID           networkid.MessageID
		PartID       networkid.PartID
		MXID         id.EventID
		RoomID       networkid.PortalID
		RoomReceiver networkid.UserLoginID
		SenderID     networkid.UserID
		Timestamp    time.Time
		Metadata     *waid.MessageMetadata
	}

	var messages []*messageInfo
	var totalScanned int

	for rows.Next() {
		totalScanned++
		var metadataBytes []byte
		var timestampNs int64
		msg := &messageInfo{
			Metadata: &waid.MessageMetadata{},
		}

		if err := rows.Scan(
			&msg.BridgeID,
			&msg.ID,
			&msg.PartID,
			&msg.MXID,
			&msg.RoomID,
			&msg.RoomReceiver,
			&msg.SenderID,
			&timestampNs,
			&metadataBytes,
		); err != nil {
			log.Err(err).Msg("Failed to scan message row")
			continue
		}

		msg.Timestamp = time.Unix(0, timestampNs)

		log.Debug().
			Str("message_id", string(msg.ID)).
			Str("room_id", string(msg.RoomID)).
			Time("timestamp", msg.Timestamp).
			Int("metadata_bytes_length", len(metadataBytes)).
			Str("metadata_preview", string(metadataBytes)).
			Msg("Scanned message row")

		// Unmarshal metadata
		if len(metadataBytes) > 0 {
			if err := json.Unmarshal(metadataBytes, msg.Metadata); err != nil {
				log.Err(err).Msg("Failed to unmarshal message metadata")
				continue
			}
		}

		// Skip messages without media metadata (text messages, etc.)
		if msg.Metadata.DirectMediaMeta == nil && msg.Metadata.FailedMediaMeta == nil {
			log.Debug().
				Str("message_id", string(msg.ID)).
				Time("timestamp", msg.Timestamp).
				Msg("Skipping message without media metadata")
			continue
		}

		log.Info().
			Str("message_id", string(msg.ID)).
			Str("room_id", string(msg.RoomID)).
			Time("timestamp", msg.Timestamp).
			Bool("has_direct_media", msg.Metadata.DirectMediaMeta != nil).
			Bool("has_failed_media", msg.Metadata.FailedMediaMeta != nil).
			Msg("Found message with media metadata")

		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("error iterating messages: %w", err)
	}

	log.Info().
		Int("total_scanned", totalScanned).
		Int("messages_with_media", len(messages)).
		Msg("Finished scanning messages")

	if len(messages) == 0 {
		return 0, 0, fmt.Errorf("no messages with media metadata found in date range (scanned %d total messages)", totalScanned)
	}

	// Process each message
	for i, msg := range messages {
		log.Info().
			Int("progress", i+1).
			Int("total", len(messages)).
			Str("message_id", string(msg.ID)).
			Time("timestamp", msg.Timestamp).
			Msg("Requesting media from phone")

		// Parse the message ID to get WhatsApp message info
		parsedMsgID, err := waid.ParseMessageID(msg.ID)
		if err != nil {
			log.Err(err).Str("message_id", string(msg.ID)).Msg("Failed to parse message ID")
			failCount++
			continue
		}

		// Get the encryption key
		var key []byte
		if msg.Metadata.DirectMediaMeta != nil {
			var keys msgconv.FailedMediaKeys
			if err := json.Unmarshal(msg.Metadata.DirectMediaMeta, &keys); err != nil {
				log.Err(err).Msg("Failed to unmarshal direct media keys")
				failCount++
				continue
			}
			key = keys.Key
		} else if msg.Metadata.FailedMediaMeta != nil {
			var preparedMedia msgconv.PreparedMedia
			if err := json.Unmarshal(msg.Metadata.FailedMediaMeta, &preparedMedia); err != nil {
				log.Err(err).Msg("Failed to unmarshal failed media metadata")
				failCount++
				continue
			}
			key = preparedMedia.FailedKeys.Key
		}

		if len(key) == 0 {
			log.Warn().Str("message_id", string(msg.ID)).Msg("No encryption key found in metadata")
			failCount++
			continue
		}

		// Send media retry request to phone
		phoneCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err = wa.Client.SendMediaRetryReceipt(phoneCtx, &types.MessageInfo{
			ID: parsedMsgID.ID,
			MessageSource: types.MessageSource{
				IsFromMe: parsedMsgID.Sender.User == wa.JID.User,
				IsGroup:  parsedMsgID.Chat.Server != types.DefaultUserServer && parsedMsgID.Chat.Server != types.BotServer,
				Sender:   parsedMsgID.Sender,
				Chat:     parsedMsgID.Chat,
			},
		}, key)
		cancel()

		if err != nil {
			log.Err(err).Str("message_id", string(msg.ID)).Msg("Failed to send media retry request to phone")
			failCount++
			continue
		}

		log.Info().
			Str("message_id", string(msg.ID)).
			Str("chat", parsedMsgID.Chat.String()).
			Str("sender", parsedMsgID.Sender.String()).
			Msg("Successfully sent media retry request to phone")

		successCount++

		// Small delay to avoid overwhelming the phone
		time.Sleep(100 * time.Millisecond)
	}

	return successCount, failCount, nil
}
