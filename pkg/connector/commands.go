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
	"errors"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

var (
	HelpSectionInvites = commands.HelpSection{Name: "Group invites", Order: 25}
)

var cmdAccept = &commands.FullHandler{
	Func: fnAccept,
	Name: "accept",
	Help: commands.HelpMeta{
		Section:     HelpSectionInvites,
		Description: "Accept a group invite. This can only be used in reply to a group invite message.",
	},
	RequiresLogin:  true,
	RequiresPortal: true,
}

func fnAccept(ce *commands.Event) {
	if len(ce.ReplyTo) == 0 {
		ce.Reply("You must reply to a group invite message when using this command.")
	} else if message, err := ce.Bridge.DB.Message.GetPartByMXID(ce.Ctx, ce.ReplyTo); err != nil {
		ce.Log.Err(err).Stringer("reply_to_mxid", ce.ReplyTo).Msg("Failed to get reply target event to handle !wa accept command")
		ce.Reply("Failed to get reply event")
	} else if message == nil {
		ce.Log.Warn().Stringer("reply_to_mxid", ce.ReplyTo).Msg("Reply target event not found to handle !wa accept command")
		ce.Reply("Reply event not found")
	} else if meta := message.Metadata.(*waid.MessageMetadata).GroupInvite; meta == nil {
		ce.Reply("That doesn't look like a group invite message.")
	} else if meta.Inviter.User == waid.ParseUserLoginID(ce.Portal.Receiver, 0).User {
		ce.Reply("You can't accept your own invites")
	} else if login := ce.Bridge.GetCachedUserLoginByID(ce.Portal.Receiver); login == nil {
		ce.Reply("Login not found")
	} else if !login.Client.IsLoggedIn() {
		ce.Reply("Not logged in")
	} else if err = login.Client.(*WhatsAppClient).Client.JoinGroupWithInvite(ce.Ctx, meta.JID, meta.Inviter, meta.Code, meta.Expiration); err != nil {
		ce.Log.Err(err).Msg("Failed to accept group invite")
		ce.Reply("Failed to accept group invite: %v", err)
	} else {
		ce.Reply("Successfully accepted the invite, the portal should be created momentarily")
	}
}

var cmdSync = &commands.FullHandler{
	Func: fnSync,
	Name: "sync",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionAdmin,
		Description: "Sync data from WhatsApp.",
		Args:        "<group/groups/contacts>",
	},
	RequiresLogin: true,
}

func fnSync(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}
	if len(ce.Args) == 0 {
		ce.Reply("Usage: `$cmdprefix sync <group/groups/contacts/contacts-with-avatars/appstate>`")
		return
	}
	logContext := func(c zerolog.Context) zerolog.Context {
		return c.Stringer("triggered_by_user", ce.User.MXID)
	}
	wa := login.Client.(*WhatsAppClient)
	switch strings.ToLower(ce.Args[0]) {
	case "group", "portal", "room":
		if ce.Portal == nil {
			ce.Reply("`!wa sync group` can only be used in a portal room.")
			return
		}
		login.QueueRemoteEvent(&simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type:       bridgev2.RemoteEventChatResync,
				PortalKey:  ce.Portal.PortalKey,
				LogContext: logContext,
			},
			GetChatInfoFunc: wa.GetChatInfo,
		})
		ce.React("✅")
	case "groups":
		groups, err := wa.Client.GetJoinedGroups(ce.Ctx)
		if err != nil {
			ce.Reply("Failed to get joined groups: %v", err)
			return
		}
		for _, group := range groups {
			login.QueueRemoteEvent(&simplevent.ChatResync{
				EventMeta: simplevent.EventMeta{
					Type:         bridgev2.RemoteEventChatResync,
					PortalKey:    wa.makeWAPortalKey(group.JID),
					LogContext:   logContext,
					CreatePortal: true,
				},
				GetChatInfoFunc: func(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
					wrapped := wa.wrapGroupInfo(ce.Ctx, group)
					wrapped.ExtraUpdates = bridgev2.MergeExtraUpdaters(wrapped.ExtraUpdates, updatePortalLastSyncAt)
					wa.addExtrasToWrapped(ce.Ctx, group.JID, wrapped, nil, portal.MXID == "")
					return wrapped, nil
				},
			})
		}
		ce.Reply("Queued syncs for %d groups", len(groups))
	case "contacts":
		wa.resyncContacts(false, false)
		ce.React("✅")
	case "contacts-with-avatars":
		wa.resyncContacts(true, false)
		ce.React("✅")
	case "appstate":
		for _, name := range appstate.AllPatchNames {
			err := wa.Client.FetchAppState(ce.Ctx, name, true, false)
			if errors.Is(err, appstate.ErrKeyNotFound) {
				ce.Reply("Key not found error syncing app state %s: %v\n\nKey requests are sent automatically, and the sync should happen in the background after your phone responds.", name, err)
				return
			} else if err != nil {
				ce.Reply("Error syncing app state %s: %v", name, err)
			} else if name == appstate.WAPatchCriticalUnblockLow {
				ce.Reply("Synced app state %s, contact sync running in background", name)
			} else {
				ce.Reply("Synced app state %s", name)
			}
		}
	default:
		ce.Reply("Unknown sync target `%s`", ce.Args[0])
	}
}

var cmdInviteLink = &commands.FullHandler{
	Func: fnInviteLink,
	Name: "invite-link",
	Help: commands.HelpMeta{
		Section:     HelpSectionInvites,
		Description: "Get an invite link to the current group chat, optionally regenerating the link and revoking the old link.",
		Args:        "[--reset]",
	},
	RequiresPortal: true,
	RequiresLogin:  true,
}

func fnInviteLink(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}
	portalJID, err := waid.ParsePortalID(ce.Portal.ID)
	if err != nil {
		ce.Reply("Failed to parse portal ID: %v", err)
		return
	}

	wa := login.Client.(*WhatsAppClient)
	reset := len(ce.Args) > 0 && strings.ToLower(ce.Args[0]) == "--reset"
	if portalJID.Server == types.DefaultUserServer || portalJID.Server == types.HiddenUserServer {
		ce.Reply("Can't get invite link to private chat")
	} else if portalJID.IsBroadcastList() {
		ce.Reply("Can't get invite link to broadcast list")
	} else if link, err := wa.Client.GetGroupInviteLink(ce.Ctx, portalJID, reset); err != nil {
		ce.Reply("Failed to get invite link: %v", err)
	} else {
		ce.Reply(link)
	}
}

var cmdResolveLink = &commands.FullHandler{
	Func: fnResolveLink,
	Name: "resolve-link",
	Help: commands.HelpMeta{
		Section:     HelpSectionInvites,
		Description: "Resolve a WhatsApp group invite or business message link.",
		Args:        "<_group, contact, or message link_>",
	},
	RequiresLogin: true,
}

func fnResolveLink(ce *commands.Event) {
	if len(ce.Args) == 0 {
		ce.Reply("**Usage:** `$cmdprefix resolve-link <group or message link>`")
		return
	}
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}
	wa := login.Client.(*WhatsAppClient)
	if strings.HasPrefix(ce.Args[0], whatsmeow.InviteLinkPrefix) {
		group, err := wa.Client.GetGroupInfoFromLink(ce.Ctx, ce.Args[0])
		if err != nil {
			ce.Reply("Failed to get group info: %v", err)
			return
		}
		ce.Reply("That invite link points at %s (`%s`)", group.Name, group.JID)
	} else if strings.HasPrefix(ce.Args[0], whatsmeow.BusinessMessageLinkPrefix) || strings.HasPrefix(ce.Args[0], whatsmeow.BusinessMessageLinkDirectPrefix) {
		target, err := wa.Client.ResolveBusinessMessageLink(ce.Ctx, ce.Args[0])
		if err != nil {
			ce.Reply("Failed to get business info: %v", err)
			return
		}
		message := ""
		if len(target.Message) > 0 {
			parts := strings.Split(target.Message, "\n")
			for i, part := range parts {
				parts[i] = "> " + html.EscapeString(part)
			}
			message = fmt.Sprintf(" The following prefilled message is attached:\n\n%s", strings.Join(parts, "\n"))
		}
		ce.Reply("That link points at %s (+%s).%s", target.PushName, target.JID.User, message)
	} else if strings.HasPrefix(ce.Args[0], whatsmeow.ContactQRLinkPrefix) || strings.HasPrefix(ce.Args[0], whatsmeow.ContactQRLinkDirectPrefix) {
		target, err := wa.Client.ResolveContactQRLink(ce.Ctx, ce.Args[0])
		if err != nil {
			ce.Reply("Failed to get contact info: %v", err)
			return
		}
		if target.PushName != "" {
			ce.Reply("That link points at %s (+%s)", target.PushName, target.JID.User)
		} else {
			ce.Reply("That link points at +%s", target.JID.User)
		}
	} else {
		ce.Reply("That doesn't look like a group invite link nor a business message link.")
	}
}

var cmdRedownloadMedia = &commands.FullHandler{
	Func: fnRedownloadMedia,
	Name: "redownload-media",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionAdmin,
		Description: "Re-download media attachments for a specific phone number or group JID from the past N months (default: 2 months).",
		Args:        "<phone number or group JID> [months]",
	},
	RequiresLogin: true,
}

var cmdRedownloadMediaDateRange = &commands.FullHandler{
	Func: fnRedownloadMediaDateRange,
	Name: "redownload-media-range",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionAdmin,
		Description: "Re-download media attachments for a specific phone number or group JID within a date range.",
		Args:        "<phone number or group JID> <start_date> <end_date>",
	},
	RequiresLogin: true,
}

var cmdRedownloadAllMedia = &commands.FullHandler{
	Func: fnRedownloadAllMedia,
	Name: "redownload-all-media",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionAdmin,
		Description: "Re-download media attachments from ALL contacts and groups from the past N months (default: 2 months).",
		Args:        "[months]",
	},
	RequiresLogin: true,
}

var cmdRedownloadAllMediaDateRange = &commands.FullHandler{
	Func: fnRedownloadAllMediaDateRange,
	Name: "redownload-all-media-range",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionAdmin,
		Description: "Re-download media attachments from ALL contacts and groups within a date range.",
		Args:        "<start_date> <end_date>",
	},
	RequiresLogin: true,
}

var cmdRedownloadAllMessagesDateRange = &commands.FullHandler{
	Func: fnRedownloadAllMessagesDateRange,
	Name: "redownload-all-messages-range",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionAdmin,
		Description: "Re-download ALL messages (including successfully uploaded media) from ALL contacts and groups within a date range. Use this if you lost Synapse media store.",
		Args:        "<start_date> <end_date>",
	},
	RequiresLogin: true,
}

var cmdDebugMediaMessages = &commands.FullHandler{
	Func: fnDebugMediaMessages,
	Name: "debug-media",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionGeneral,
		Description: "Debug media messages in the database for a specific phone number",
		Args:        "<phone number> [months]",
	},
	RequiresLogin: true,
}

var cmdListMediaContacts = &commands.FullHandler{
	Func: fnListMediaContacts,
	Name: "list-media-contacts",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionGeneral,
		Description: "List contacts/chats that have media messages in the database",
		Args:        "[months]",
	},
	RequiresLogin: true,
}

func fnRedownloadMedia(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}

	wa := login.Client.(*WhatsAppClient)

	// Parse phone number or group JID parameter (required)
	if len(ce.Args) < 1 {
		ce.Reply("Usage: redownload-media <phone number or group JID> [months]\n\nExamples:\n  redownload-media +1234567890 2\n  redownload-media 120363423845693710@g.us 2\n  redownload-media 120363423845693710 2")
		return
	}

	identifier := ce.Args[0]

	// Parse the identifier into a JID
	// Support formats:
	// - 14253262350 (phone number)
	// - +14253262350 (phone number with +)
	// - 14253262350@s.whatsapp.net (full phone JID)
	// - 120363423845693710 (group ID)
	// - 120363423845693710@g.us (full group JID)
	var jid types.JID
	var err error

	// Try parsing as a full JID first (e.g., 1234567890@s.whatsapp.net or 120...@g.us)
	if strings.Contains(identifier, "@") {
		jid, err = types.ParseJID(identifier)
		if err != nil {
			ce.Reply("Invalid JID format: %v", err)
			return
		}
	} else {
		// Remove + prefix if present
		identifier = strings.TrimPrefix(identifier, "+")

		// Determine if it's a group ID or phone number
		// Group IDs are typically 15+ digits and often start with "120"
		if len(identifier) >= 15 && (strings.HasPrefix(identifier, "120") || strings.HasPrefix(identifier, "12")) {
			// Likely a group ID - add @g.us
			jid = types.NewJID(identifier, types.GroupServer)
		} else {
			// Likely a phone number - add @s.whatsapp.net
			jid = types.NewJID(identifier, types.DefaultUserServer)
		}
	}

	// Convert JID to portal ID
	portalID := waid.MakePortalID(jid)

	// Parse months argument (default: 2)
	months := 2
	if len(ce.Args) > 1 {
		var err error
		_, err = fmt.Sscanf(ce.Args[1], "%d", &months)
		if err != nil || months < 1 || months > 12 {
			ce.Reply("Invalid months value. Please provide a number between 1 and 12.")
			return
		}
	}

	ce.Reply("Searching for media messages from %s in the past %d month(s)...", jid.String(), months)

	// Get media messages from the past N months for the specific portal
	messages, err := wa.GetMediaMessagesFromPastMonths(ce.Ctx, months, portalID)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to get media messages")
		ce.Reply("Failed to query media messages: %v", err)
		return
	}

	if len(messages) == 0 {
		ce.Reply("No media messages found from %s in the past %d month(s).", jid.String(), months)
		return
	}

	ce.Reply("Found %d media message(s) from %s. Sending retry requests to WhatsApp...", len(messages), jid.String())

	// Send media retry requests
	successCount, failCount, err := wa.RedownloadMediaForMessages(ce.Ctx, messages)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to redownload media")
		ce.Reply("Error during media redownload: %v", err)
		return
	}

	ce.Reply(
		"Media redownload complete for %s!\n"+
			"✅ Successfully requested: %d\n"+
			"❌ Failed: %d\n\n"+
			"Note: WhatsApp will send the media asynchronously. "+
			"Media should appear in your rooms within a few minutes if available.",
		jid.String(),
		successCount,
		failCount,
	)
}

func fnRedownloadMediaDateRange(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}

	wa := login.Client.(*WhatsAppClient)

	// Parse arguments: <phone_number_or_group_jid> <start_date> <end_date>
	// Arguments can be:
	// - 3 args: phone "2025-11-21" "2025-12-02"
	// - 5 args: phone "2025-11-21" "19:13:34" "2025-12-02" "17:31:55"
	if len(ce.Args) < 3 {
		ce.Reply("Usage: redownload-media-range <phone number or group JID> <start_date> <end_date>\n\n" +
			"Date format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS\n\n" +
			"Examples:\n" +
			"  redownload-media-range 14253262350 2025-11-21 2025-12-02\n" +
			"  redownload-media-range 120363423845693710@g.us 2025-11-21 19:13:34 2025-12-02 17:31:55")
		return
	}

	identifier := ce.Args[0]
	var startDateStr, endDateStr string

	// Check if we have time components (5+ args) or just dates (3-4 args)
	if len(ce.Args) >= 5 {
		// Format: phone YYYY-MM-DD HH:MM:SS YYYY-MM-DD HH:MM:SS
		startDateStr = ce.Args[1] + " " + ce.Args[2]
		endDateStr = ce.Args[3] + " " + ce.Args[4]
	} else if len(ce.Args) == 4 {
		// Ambiguous - could be "phone date time date" or "phone date date time"
		// Try to parse to determine
		_, err1 := time.Parse("2006-01-02", ce.Args[1])
		_, err2 := time.Parse("15:04:05", ce.Args[2])
		if err1 == nil && err2 == nil {
			// Second is date, third is time, fourth is date
			startDateStr = ce.Args[1] + " " + ce.Args[2]
			endDateStr = ce.Args[3]
		} else {
			// Assume second and third are dates
			startDateStr = ce.Args[1]
			endDateStr = ce.Args[2] + " " + ce.Args[3]
		}
	} else {
		// Just 3 args - phone and two dates
		startDateStr = ce.Args[1]
		endDateStr = ce.Args[2]
	}

	// Parse the identifier into a JID (same logic as fnRedownloadMedia)
	var jid types.JID
	var err error

	if strings.Contains(identifier, "@") {
		jid, err = types.ParseJID(identifier)
		if err != nil {
			ce.Reply("Invalid JID format: %v", err)
			return
		}
	} else {
		identifier = strings.TrimPrefix(identifier, "+")
		if len(identifier) >= 15 && (strings.HasPrefix(identifier, "120") || strings.HasPrefix(identifier, "12")) {
			jid = types.NewJID(identifier, types.GroupServer)
		} else {
			jid = types.NewJID(identifier, types.DefaultUserServer)
		}
	}

	// Convert JID to portal ID
	portalID := waid.MakePortalID(jid)

	// Parse start date
	var startTime time.Time
	// Try parsing with time first (YYYY-MM-DD HH:MM:SS)
	startTime, err = time.Parse("2006-01-02 15:04:05", startDateStr)
	if err != nil {
		// Try parsing date only (YYYY-MM-DD)
		startTime, err = time.Parse("2006-01-02", startDateStr)
		if err != nil {
			ce.Reply("Invalid start date format. Use YYYY-MM-DD or \"YYYY-MM-DD HH:MM:SS\"")
			return
		}
	}

	// Parse end date
	var endTime time.Time
	endTime, err = time.Parse("2006-01-02 15:04:05", endDateStr)
	if err != nil {
		endTime, err = time.Parse("2006-01-02", endDateStr)
		if err != nil {
			ce.Reply("Invalid end date format. Use YYYY-MM-DD or \"YYYY-MM-DD HH:MM:SS\"")
			return
		}
		// If only date provided for end time, set to end of day
		endTime = endTime.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}

	// Validate date range
	if endTime.Before(startTime) {
		ce.Reply("End date must be after start date")
		return
	}

	ce.Reply("Searching for media messages from %s between %s and %s...",
		jid.String(),
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	// Get media messages in the date range
	messages, err := wa.GetMediaMessagesInDateRange(ce.Ctx, startTime, endTime, portalID)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to get media messages")
		ce.Reply("Failed to query media messages: %v", err)
		return
	}

	if len(messages) == 0 {
		ce.Reply("No media messages found from %s in the specified date range.", jid.String())
		return
	}

	ce.Reply("Found %d media message(s) from %s. Downloading and re-uploading to Synapse...", len(messages), jid.String())

	// Redownload media
	successCount, failCount, err := wa.RedownloadMediaForMessages(ce.Ctx, messages)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to redownload media")
		ce.Reply("Error during media redownload: %v", err)
		return
	}

	ce.Reply(
		"Media redownload complete for %s!\n"+
			"✅ Successfully redownloaded: %d\n"+
			"❌ Failed: %d\n\n"+
			"Date range: %s to %s",
		jid.String(),
		successCount,
		failCount,
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"),
	)
}

func fnRedownloadAllMedia(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("You are not logged in.")
		return
	}
	wa := login.Client.(*WhatsAppClient)

	// Parse months parameter (default: 2, range: 1-12)
	months := 2
	if len(ce.Args) > 0 {
		_, err := fmt.Sscanf(ce.Args[0], "%d", &months)
		if err != nil || months < 1 || months > 12 {
			ce.Reply("Invalid months value. Please provide a number between 1 and 12.")
			return
		}
	}

	ce.Reply("Finding all contacts and groups with media messages from the past %d month(s)...", months)

	// Calculate the time threshold (N months ago)
	threshold := time.Now().AddDate(0, -months, 0)
	thresholdNs := threshold.UnixNano()

	// Query to get all chats with media messages
	query := `
		SELECT room_id, COUNT(*) as media_count
		FROM message
		WHERE bridge_id = $1
		  AND timestamp >= $2
		  AND (
		      metadata::text LIKE '%"direct_media_meta"%'
		      OR metadata::text LIKE '%"media_meta"%'
		  )
		GROUP BY room_id
		ORDER BY media_count DESC
	`

	rows, err := wa.Main.Bridge.DB.Query(ce.Ctx, query, wa.Main.Bridge.ID, thresholdNs)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to query media contacts")
		ce.Reply("Failed to query media contacts: %v", err)
		return
	}
	defer rows.Close()

	type chatInfo struct {
		roomID     string
		mediaCount int
	}
	var chats []chatInfo

	for rows.Next() {
		var roomID string
		var mediaCount int
		err := rows.Scan(&roomID, &mediaCount)
		if err != nil {
			ce.Log.Err(err).Msg("Failed to scan row")
			continue
		}
		chats = append(chats, chatInfo{roomID: roomID, mediaCount: mediaCount})
	}

	if len(chats) == 0 {
		ce.Reply("No media messages found in any chats from the past %d month(s).", months)
		return
	}

	ce.Reply("Found %d chat(s) with media messages. Starting redownload...", len(chats))

	totalSuccess := 0
	totalFailed := 0
	processedChats := 0

	for _, chat := range chats {
		portalID := networkid.PortalID(chat.roomID)

		ce.Log.Info().
			Str("portal_id", string(portalID)).
			Int("media_count", chat.mediaCount).
			Msg("Processing chat for media redownload")

		// Get media messages for this chat
		messages, err := wa.GetMediaMessagesFromPastMonths(ce.Ctx, months, portalID)
		if err != nil {
			ce.Log.Err(err).Str("portal_id", string(portalID)).Msg("Failed to get media messages")
			continue
		}

		if len(messages) == 0 {
			continue
		}

		// Redownload media for this chat
		successCount, failCount, err := wa.RedownloadMediaForMessages(ce.Ctx, messages)
		if err != nil {
			ce.Log.Err(err).Str("portal_id", string(portalID)).Msg("Failed to redownload media")
			continue
		}

		totalSuccess += successCount
		totalFailed += failCount
		processedChats++

		ce.Log.Info().
			Str("portal_id", string(portalID)).
			Int("success", successCount).
			Int("failed", failCount).
			Msg("Completed media redownload for chat")
	}

	ce.Reply(
		"Media redownload complete for all chats!\n"+
			"📊 Processed %d chat(s)\n"+
			"✅ Successfully redownloaded: %d\n"+
			"❌ Failed: %d\n\n"+
			"Note: Media should appear in your rooms within a few minutes if available.",
		processedChats,
		totalSuccess,
		totalFailed,
	)
}

func fnRedownloadAllMediaDateRange(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}
	wa := login.Client.(*WhatsAppClient)

	// Parse arguments: <start_date> <end_date>
	// Arguments can be:
	// - 2 args: "2025-11-21" "2025-12-02"
	// - 4 args: "2025-11-21" "19:13:34" "2025-12-02" "17:31:55"
	if len(ce.Args) < 2 {
		ce.Reply("Usage: redownload-all-media-range <start_date> <end_date>\n\n" +
			"Date format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS\n\n" +
			"Examples:\n" +
			"  redownload-all-media-range 2025-11-21 2025-12-02\n" +
			"  redownload-all-media-range 2025-11-21 19:13:34 2025-12-02 17:31:55")
		return
	}

	var startDateStr, endDateStr string

	// Check if we have time components (4+ args) or just dates (2-3 args)
	if len(ce.Args) >= 4 {
		// Format: YYYY-MM-DD HH:MM:SS YYYY-MM-DD HH:MM:SS
		startDateStr = ce.Args[0] + " " + ce.Args[1]
		endDateStr = ce.Args[2] + " " + ce.Args[3]
	} else if len(ce.Args) == 3 {
		// Ambiguous - could be "date time date" or "date date time"
		// Try to parse to determine
		_, err1 := time.Parse("2006-01-02", ce.Args[0])
		_, err2 := time.Parse("15:04:05", ce.Args[1])
		if err1 == nil && err2 == nil {
			// First is date, second is time, third is date
			startDateStr = ce.Args[0] + " " + ce.Args[1]
			endDateStr = ce.Args[2]
		} else {
			// Assume first two are dates
			startDateStr = ce.Args[0]
			endDateStr = ce.Args[1] + " " + ce.Args[2]
		}
	} else {
		// Just 2 args - both are dates
		startDateStr = ce.Args[0]
		endDateStr = ce.Args[1]
	}

	// Parse start date
	var startTime time.Time
	var err error
	startTime, err = time.Parse("2006-01-02 15:04:05", startDateStr)
	if err != nil {
		startTime, err = time.Parse("2006-01-02", startDateStr)
		if err != nil {
			ce.Reply("Invalid start date format. Use YYYY-MM-DD or \"YYYY-MM-DD HH:MM:SS\"")
			return
		}
	}

	// Parse end date
	var endTime time.Time
	endTime, err = time.Parse("2006-01-02 15:04:05", endDateStr)
	if err != nil {
		endTime, err = time.Parse("2006-01-02", endDateStr)
		if err != nil {
			ce.Reply("Invalid end date format. Use YYYY-MM-DD or \"YYYY-MM-DD HH:MM:SS\"")
			return
		}
		// If only date provided for end time, set to end of day
		endTime = endTime.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}

	// Validate date range
	if endTime.Before(startTime) {
		ce.Reply("End date must be after start date")
		return
	}

	ce.Reply("Finding all contacts and groups with media messages between %s and %s...",
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	// Calculate timestamps in nanoseconds
	startNs := startTime.UnixNano()
	endNs := endTime.UnixNano()

	// Query to get all chats with media messages in the date range
	query := `
		SELECT room_id, COUNT(*) as media_count
		FROM message
		WHERE bridge_id = $1
		  AND timestamp >= $2
		  AND timestamp <= $3
		  AND (
		      metadata::text LIKE '%"direct_media_meta"%'
		      OR metadata::text LIKE '%"media_meta"%'
		  )
		GROUP BY room_id
		ORDER BY media_count DESC
	`

	rows, err := wa.Main.Bridge.DB.Query(ce.Ctx, query, wa.Main.Bridge.ID, startNs, endNs)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to query media contacts")
		ce.Reply("Failed to query media contacts: %v", err)
		return
	}
	defer rows.Close()

	type chatInfo struct {
		roomID     string
		mediaCount int
	}
	var chats []chatInfo

	for rows.Next() {
		var roomID string
		var mediaCount int
		err := rows.Scan(&roomID, &mediaCount)
		if err != nil {
			ce.Log.Err(err).Msg("Failed to scan row")
			continue
		}
		chats = append(chats, chatInfo{roomID: roomID, mediaCount: mediaCount})
	}

	if len(chats) == 0 {
		ce.Reply("No media messages found in any chats in the specified date range.")
		return
	}

	ce.Reply("Found %d chat(s) with media messages. Starting redownload...", len(chats))

	totalSuccess := 0
	totalFailed := 0
	processedChats := 0

	for _, chat := range chats {
		portalID := networkid.PortalID(chat.roomID)

		ce.Log.Info().
			Str("portal_id", string(portalID)).
			Int("media_count", chat.mediaCount).
			Msg("Processing chat for media redownload")

		// Get media messages for this chat in the date range
		messages, err := wa.GetMediaMessagesInDateRange(ce.Ctx, startTime, endTime, portalID)
		if err != nil {
			ce.Log.Err(err).Str("portal_id", string(portalID)).Msg("Failed to get media messages")
			continue
		}

		if len(messages) == 0 {
			continue
		}

		// Redownload media for this chat
		successCount, failCount, err := wa.RedownloadMediaForMessages(ce.Ctx, messages)
		if err != nil {
			ce.Log.Err(err).Str("portal_id", string(portalID)).Msg("Failed to redownload media")
			continue
		}

		totalSuccess += successCount
		totalFailed += failCount
		processedChats++

		ce.Log.Info().
			Str("portal_id", string(portalID)).
			Int("success", successCount).
			Int("failed", failCount).
			Msg("Completed media redownload for chat")
	}

	ce.Reply(
		"Media redownload complete for all chats!\n"+
			"📊 Processed %d chat(s)\n"+
			"✅ Successfully redownloaded: %d\n"+
			"❌ Failed: %d\n\n"+
			"Date range: %s to %s",
		processedChats,
		totalSuccess,
		totalFailed,
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"),
	)
}

func fnRedownloadAllMessagesDateRange(ce *commands.Event) {
	if len(ce.Args) < 2 {
		ce.Reply("Usage: `!wa redownload-all-messages-range <start_date> <end_date>`\n\n" +
			"This command finds ALL messages in the date range and requests media from your phone.\n" +
			"Use this if you lost your Synapse media store during a specific timeframe.\n\n" +
			"⚠️ **Requirements:**\n" +
			"- Your phone must be online\n" +
			"- Your phone must still have the media files\n" +
			"- Media must be less than ~30 days old (WhatsApp retention policy)\n\n" +
			"Examples:\n" +
			"- `!wa redownload-all-messages-range 2025-11-21 2025-12-02`\n" +
			"- `!wa redownload-all-messages-range 2025-11-21 19:13:34 2025-12-02 17:31:55`")
		return
	}

	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}

	wa := login.Client.(*WhatsAppClient)

	// Parse date arguments (same logic as fnRedownloadAllMediaDateRange)
	var startDateStr, endDateStr string

	if len(ce.Args) >= 4 {
		startDateStr = ce.Args[0] + " " + ce.Args[1]
		endDateStr = ce.Args[2] + " " + ce.Args[3]
	} else if len(ce.Args) == 3 {
		_, err1 := time.Parse("2006-01-02", ce.Args[0])
		_, err2 := time.Parse("15:04:05", ce.Args[1])
		if err1 == nil && err2 == nil {
			startDateStr = ce.Args[0] + " " + ce.Args[1]
			endDateStr = ce.Args[2]
		} else {
			startDateStr = ce.Args[0]
			endDateStr = ce.Args[1] + " " + ce.Args[2]
		}
	} else {
		startDateStr = ce.Args[0]
		endDateStr = ce.Args[1]
	}

	var startTime time.Time
	var err error
	startTime, err = time.Parse("2006-01-02 15:04:05", startDateStr)
	if err != nil {
		startTime, err = time.Parse("2006-01-02", startDateStr)
		if err != nil {
			ce.Reply("Invalid start date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
			return
		}
	}

	var endTime time.Time
	endTime, err = time.Parse("2006-01-02 15:04:05", endDateStr)
	if err != nil {
		endTime, err = time.Parse("2006-01-02", endDateStr)
		if err != nil {
			ce.Reply("Invalid end date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
			return
		}
		endTime = endTime.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}

	ce.Reply("🔍 Searching for messages with media from %s to %s...",
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	// First try history sync table (if available)
	historyMessages, err := wa.GetHistorySyncMediaMessagesInDateRange(ce.Ctx, startTime, endTime)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to query history sync messages")
	}

	if len(historyMessages) > 0 {
		ce.Reply("📥 Found %d media message(s) in history sync. Downloading from WhatsApp CDN...", len(historyMessages))

		successCount, failCount, err := wa.RedownloadHistorySyncMedia(ce.Ctx, historyMessages)
		if err != nil {
			ce.Reply("❌ Error during redownload: %v", err)
			return
		}

		ce.Reply(
			"✅ Media redownload from history sync complete!\n"+
				"✅ Successfully redownloaded: %d\n"+
				"❌ Failed: %d\n\n"+
				"Date range: %s to %s\n\n"+
				"⚠️ **Note:** The Matrix messages still have the old MXC URLs. "+
				"You may need to manually edit the messages or redact and resend them.",
			successCount,
			failCount,
			startTime.Format("2006-01-02 15:04:05"),
			endTime.Format("2006-01-02 15:04:05"),
		)
		return
	}

	// History sync table is empty, try requesting from phone
	ce.Reply("📱 History sync table is empty. Requesting media from your phone instead...\n\n" +
		"⚠️ **Requirements:**\n" +
		"- Your phone must be online\n" +
		"- Your phone must still have the media files\n" +
		"- This will send retry requests to your phone\n" +
		"- The bridge will automatically download media when your phone responds\n\n" +
		"Checking media backfill request table first...")

	// Try backfill table first (contains keys for failed media during history sync)
	successCount, failCount, err := wa.RequestMediaFromBackfillTable(ce.Ctx, startTime, endTime)
	if err != nil {
		ce.Log.Warn().Err(err).Msg("Failed to use backfill table, trying phone request")
		ce.Reply("📱 Backfill table empty. Requesting media from your phone instead...")

		// Fallback to requesting from phone
		successCount, failCount, err = wa.RequestMediaFromPhone(ce.Ctx, startTime, endTime)
		if err != nil {
			ce.Reply("❌ Error requesting media from phone: %v", err)
			return
		}
	}

	if successCount == 0 && failCount == 0 {
		ce.Reply("❌ **No media messages found in the date range**\n\n" +
			"This could mean:\n" +
			"- No media was sent/received in this timeframe\n" +
			"- Media was successfully uploaded and encryption keys were discarded\n" +
			"- Messages don't have media metadata\n\n" +
			"**Your Options:**\n\n" +
			"1️⃣ **Re-sync from phone:**\n" +
			"   ```\n" +
			"   !wa sync groups\n" +
			"   ```\n" +
			"   This re-syncs all group messages and may restore media metadata\n\n" +
			"2️⃣ **Ask users to resend:**\n" +
			"   For important media, ask senders to resend the files")
		return
	}

	ce.Reply(
		"✅ Media retry requests sent to phone!\n"+
			"✅ Successfully sent: %d requests\n"+
			"❌ Failed: %d requests\n\n"+
			"Date range: %s to %s\n\n"+
			"📱 **What happens next:**\n"+
			"1. Your phone will receive the retry requests\n"+
			"2. Your phone will re-upload the media to WhatsApp servers\n"+
			"3. The bridge will automatically download and upload to Synapse\n"+
			"4. You'll see the media appear in Matrix messages\n\n"+
			"⏱️ **This may take a few minutes** depending on:\n"+
			"- Number of media files\n"+
			"- Your phone's internet connection\n"+
			"- File sizes\n\n"+
			"💡 **Tip:** Keep your phone online and check the bridge logs for progress.",
		successCount,
		failCount,
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"),
	)
}

func fnDebugMediaMessages(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}

	wa := login.Client.(*WhatsAppClient)

	if len(ce.Args) == 0 {
		ce.Reply("Usage: debug-media <phone number> [months]")
		return
	}

	// Parse phone number
	phoneNumber := ce.Args[0]
	var jid types.JID
	var err error

	// Try parsing as a full JID first (e.g., 1234567890@s.whatsapp.net)
	if strings.Contains(phoneNumber, "@") {
		jid, err = types.ParseJID(phoneNumber)
		if err != nil {
			ce.Reply("Invalid phone number format. Please provide a valid phone number or JID.")
			return
		}
	} else {
		// Remove + prefix if present
		phoneNumber = strings.TrimPrefix(phoneNumber, "+")
		// Create JID from phone number
		jid = types.NewJID(phoneNumber, types.DefaultUserServer)
	}

	// Convert JID to portal ID
	portalID := waid.MakePortalID(jid)

	// Parse months argument (default: 1)
	months := 1
	if len(ce.Args) > 1 {
		var err error
		_, err = fmt.Sscanf(ce.Args[1], "%d", &months)
		if err != nil || months < 1 || months > 12 {
			ce.Reply("Invalid months value. Please provide a number between 1 and 12.")
			return
		}
	}

	ce.Reply("Debugging media messages from %s in the past %d month(s)...", jid.String(), months)

	// Calculate the time threshold (N months ago)
	threshold := time.Now().AddDate(0, -months, 0)
	// Database stores timestamps in nanoseconds
	thresholdNs := threshold.UnixNano()

	ce.Log.Info().
		Time("threshold", threshold).
		Int64("threshold_ns", thresholdNs).
		Str("portal_id", string(portalID)).
		Msg("Debug query parameters")

	// Query all messages for this portal (don't filter by room_receiver for DMs)
	query := `
		SELECT bridge_id, id, part_id, mxid, room_id, room_receiver, sender_id, timestamp, metadata
		FROM message
		WHERE bridge_id = $1
		  AND room_id = $2
		  AND timestamp >= $3
		ORDER BY timestamp DESC
		LIMIT 100
	`

	rows, err := wa.Main.Bridge.DB.Query(ce.Ctx, query, wa.Main.Bridge.ID, portalID, thresholdNs)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to query messages")
		ce.Reply("Failed to query messages: %v", err)
		return
	}
	defer rows.Close()

	totalMessages := 0
	messagesWithDirectMedia := 0
	messagesWithMediaMeta := 0
	messagesWithFailedMedia := 0

	var sampleMetadata string
	for rows.Next() {
		totalMessages++
		var bridgeID, id, partID, mxid, roomID, roomReceiver, senderID string
		var timestampNs int64
		var metadataJSON []byte

		err := rows.Scan(&bridgeID, &id, &partID, &mxid, &roomID, &roomReceiver, &senderID, &timestampNs, &metadataJSON)
		if err != nil {
			ce.Log.Err(err).Msg("Failed to scan row")
			continue
		}

		// Log each message timestamp
		msgTime := time.Unix(0, timestampNs)
		ce.Log.Debug().
			Str("message_id", id).
			Time("timestamp", msgTime).
			Int64("timestamp_ns", timestampNs).
			Bool("within_range", timestampNs >= thresholdNs).
			Msg("Found message in debug query")

		metadataStr := string(metadataJSON)
		hasDirectMedia := strings.Contains(metadataStr, "direct_media_meta")
		hasMediaMeta := strings.Contains(metadataStr, "\"media_meta\"")
		hasFailedMedia := strings.Contains(metadataStr, "failed_media_meta")

		// Log metadata for first 5 messages to help debug
		if totalMessages <= 5 {
			previewLen := len(metadataStr)
			if previewLen > 200 {
				previewLen = 200
			}
			ce.Log.Info().
				Str("message_id", id).
				Time("timestamp", msgTime).
				Str("metadata_preview", metadataStr[:previewLen]).
				Bool("has_direct_media", hasDirectMedia).
				Bool("has_media_meta", hasMediaMeta).
				Bool("has_failed_media", hasFailedMedia).
				Msg("Sample message metadata")
		}

		if hasDirectMedia {
			messagesWithDirectMedia++
		}
		if hasMediaMeta {
			messagesWithMediaMeta++
		}
		if hasFailedMedia {
			messagesWithFailedMedia++
		}

		// Save first metadata sample from a message that has media_meta
		if sampleMetadata == "" && hasMediaMeta && len(metadataJSON) > 0 {
			if len(metadataStr) > 500 {
				sampleMetadata = metadataStr[:500] + "..."
			} else {
				sampleMetadata = metadataStr
			}
		}
	}

	// Also check for ANY media messages in the entire database
	allMediaQuery := `
		SELECT COUNT(*) FROM message
		WHERE bridge_id = $1
		  AND timestamp >= $2
		  AND (
		      metadata::text LIKE '%"direct_media_meta"%'
		      OR metadata::text LIKE '%"media_meta"%'
		  )
	`
	var totalMediaCount int
	err = wa.Main.Bridge.DB.QueryRow(ce.Ctx, allMediaQuery, wa.Main.Bridge.ID, thresholdNs).Scan(&totalMediaCount)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to count total media messages")
		totalMediaCount = -1
	}

	reply := fmt.Sprintf(
		"Debug results for %s:\n"+
			"📊 Total messages from contact: %d\n"+
			"📷 Messages with direct_media_meta: %d\n"+
			"📎 Messages with media_meta: %d\n"+
			"❌ Messages with failed_media_meta: %d\n"+
			"🌍 Total media messages in DB (all contacts): %d\n\n"+
			"Portal ID: %s\n"+
			"User Login ID: %s\n"+
			"Bridge ID: %s\n"+
			"Your WhatsApp JID: %s",
		jid.String(),
		totalMessages,
		messagesWithDirectMedia,
		messagesWithMediaMeta,
		messagesWithFailedMedia,
		totalMediaCount,
		portalID,
		wa.UserLogin.ID,
		wa.Main.Bridge.ID,
		wa.JID.String(),
	)

	if sampleMetadata != "" {
		reply += "\n\nSample metadata from first message:\n" + sampleMetadata
	}

	ce.Reply(reply)
}

func fnListMediaContacts(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}

	wa := login.Client.(*WhatsAppClient)

	// Parse months argument (default: 1)
	months := 1
	if len(ce.Args) > 0 {
		var err error
		_, err = fmt.Sscanf(ce.Args[0], "%d", &months)
		if err != nil || months < 1 || months > 12 {
			ce.Reply("Invalid months value. Please provide a number between 1 and 12.")
			return
		}
	}

	ce.Reply("Searching for contacts with media messages in the past %d month(s)...", months)

	// Calculate the time threshold (N months ago)
	threshold := time.Now().AddDate(0, -months, 0)
	// Database stores timestamps in nanoseconds
	thresholdNs := threshold.UnixNano()

	// Query to get contacts with media messages
	query := `
		SELECT room_id, COUNT(*) as media_count
		FROM message
		WHERE bridge_id = $1
		  AND timestamp >= $2
		  AND (
		      metadata::text LIKE '%"direct_media_meta"%'
		      OR metadata::text LIKE '%"media_meta"%'
		  )
		GROUP BY room_id
		ORDER BY media_count DESC
		LIMIT 20
	`

	rows, err := wa.Main.Bridge.DB.Query(ce.Ctx, query, wa.Main.Bridge.ID, thresholdNs)
	if err != nil {
		ce.Log.Err(err).Msg("Failed to query media contacts")
		ce.Reply("Failed to query media contacts: %v", err)
		return
	}
	defer rows.Close()

	var results []string
	totalContacts := 0
	for rows.Next() {
		var roomID string
		var mediaCount int
		err := rows.Scan(&roomID, &mediaCount)
		if err != nil {
			ce.Log.Err(err).Msg("Failed to scan row")
			continue
		}
		totalContacts++
		results = append(results, fmt.Sprintf("  %s: %d media messages", roomID, mediaCount))
	}

	if totalContacts == 0 {
		ce.Reply("No contacts with media messages found in the past %d month(s).", months)
	} else {
		reply := fmt.Sprintf("Found %d contacts with media messages:\n\n", totalContacts)
		reply += strings.Join(results, "\n")
		reply += fmt.Sprintf("\n\nTo re-download media from a contact, use:\n!wa redownload-media <phone_number> %d", months)
		ce.Reply(reply)
	}
}

var cmdJoin = &commands.FullHandler{
	Func: fnJoin,
	Name: "join",
	Help: commands.HelpMeta{
		Section:     HelpSectionInvites,
		Description: "Join a group chat with an invite link.",
		Args:        "<_invite link_>",
	},
	RequiresLogin: true,
}

func fnJoin(ce *commands.Event) {
	if len(ce.Args) == 0 {
		ce.Reply("**Usage:** `$cmdprefix join <invite link>`")
		return
	}
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Login not found")
		return
	}
	wa := login.Client.(*WhatsAppClient)

	if strings.HasPrefix(ce.Args[0], whatsmeow.InviteLinkPrefix) {
		jid, err := wa.Client.JoinGroupWithLink(ce.Ctx, ce.Args[0])
		if err != nil {
			ce.Reply("Failed to join group: %v", err)
			return
		}
		ce.Log.Debug().Stringer("group_jid", jid).Msg("User successfully joined WhatsApp group with link")
		ce.Reply("Successfully joined group `%s`, the portal should be created momentarily", jid)
	} else if strings.HasPrefix(ce.Args[0], whatsmeow.NewsletterLinkPrefix) {
		info, err := wa.Client.GetNewsletterInfoWithInvite(ce.Ctx, ce.Args[0])
		if err != nil {
			ce.Reply("Failed to get channel info: %v", err)
			return
		}
		err = wa.Client.FollowNewsletter(ce.Ctx, info.ID)
		if err != nil {
			ce.Reply("Failed to follow channel: %v", err)
			return
		}
		ce.Log.Debug().Stringer("channel_jid", info.ID).Msg("User successfully followed WhatsApp channel with link")
		ce.Reply("Successfully followed channel `%s`, the portal should be created momentarily", info.ID)
	} else {
		ce.Reply("That doesn't look like a WhatsApp invite link")
	}
}
