# Salebot / amoCRM Chat Bridge

The bridge is a separate component from the lead/contact/UTM synchronization in integrationSalebot.php. It forwards Salebot text messages through amoCRM Chats API and delivers replies from amoCRM back to the Salebot client.

## Current scope

- The configured default portal is directorchinatutorru. A second portal can be enabled after its amoCRM chat channel is registered separately.
- Text messages only. Attachments, buttons, edits, and reactions are not forwarded.
- The bridge only attaches to a contact when the existing Salebot variables contain amo_lead_id or amo_client_id; it does not create new CRM entities itself.
- Stable participant/conversation identifiers are derived from Salebot client.id, not phone or email.
- Duplicate messages are tracked by stable message IDs. Server-side state contains hashed client filenames and message IDs, not message text.
- The bridge is disabled until the issued channel credentials and scopes are added to the server environment.

## Before enabling

Register a private Chat API channel for ChinaTutor and allow the target portal accounts. The callback URL to register is:

https://srm.chinatutor.ru/amo_chat_webhook.php/:scope_id

The channel registration must include the OAuth integration `client_uuid` that will call the CRM Chat endpoints. The channel itself provides a channel ID, channel secret, and bot participant ID. Keep all credentials on the server, never in the repository.

For each portal, obtain its amojo account ID from GET /api/v4/account?with=amojo_id, then connect it once using POST /v2/origin/custom/{channel_id}/connect with account_id and hook_api_version=v2. This returns a scope_id for messaging; it does not grant the OAuth integration access to the channel for CRM chat-link methods. The helper command is:

php diagnostics/amocrm/connect_chat_channel.php directorchinatutorru

After connecting, store the returned scope ID. Repeat only after the channel has been approved for the second portal, using the secondary portal key.

## Environment settings

Set these in the server .env. Set integrationSalebot.php to the existing Salebot project webhook URL; it already invokes the bridge after resolving the contact. Do not register a second Salebot project webhook.

AMO_CHAT_DIRECTOR_ENABLED=0
AMO_CHAT_DIRECTOR_CHANNEL_ID=<issued channel id>
AMO_CHAT_DIRECTOR_CHANNEL_SECRET=<issued channel secret>
AMO_CHAT_DIRECTOR_SCOPE_ID=<scope id returned by connect>
AMO_CHAT_DIRECTOR_ACCOUNT_ID=<portal amojo id>
AMO_CHAT_DIRECTOR_BOT_REF_ID=<issued bot participant id>
AMO_CHAT_DIRECTOR_ACCESS_TOKEN=<OAuth access token issued to the registered channel client_uuid>
AMO_CHAT_SALEBOT_API_KEY=<new Salebot API key with client read and send-message permissions>
AMO_CHAT_STATE_DIR=<persistent directory outside the public web root>

Configure AMO_CHAT_SECONDARY_* only after its own channel is approved and connected. The OAuth integration UUID is required for `POST /api/v4/contacts/chats` and `GET /api/v4/contacts/chats`; it must be included in the registered channel's settings. The `connect` endpoint cannot add or change this UUID. The webhook path must reach PHP as `PATH_INFO`; verify this on the hosting configuration before registering the callback.

## Channel access prerequisite

The contacts chat-link endpoints require all three conditions: account-admin OAuth permissions, no session cookies, and the requesting integration UUID configured on the channel. The current failure `Channel must be linked to your client, please setup channel first` means the final condition is not met for the OAuth integration used by this bridge. Creating a chat through Amojo and receiving a `scope_id` do not satisfy it. The public API documents no endpoint for changing the registered channel's integration UUID or allowed-client list. Do not retry `contacts/chats` or claim the contact link succeeded until this channel/client association has been corrected.

Create a dedicated Salebot API key with read and change-client permissions. The bridge does not fall back to the legacy key embedded in `integrationSalebot.php`; rotate that exposed key separately.

Until Chat API is registered, `integrationSalebot.php` still creates a contact and lead. For a newly created lead it adds a common note with the Salebot `client_id`, `platform_id`, messenger type and bot group. If Salebot sends a real URL in `salebot_link`, `salebot_url`, `client_url`, `profile_url`, `link` or `url`, that URL is added to the note. The Salebot API does not provide a universal public client-card URL, so the code does not construct an unverified address.

## Validation

Deploy first with AMO_CHAT_DIRECTOR_ENABLED=0. After adding approved credentials, enable the primary portal and send a message from a test client that already has an amo contact/lead ID. Confirm the chat is linked to that contact, then reply from amoCRM and verify the reply reaches Salebot without looping back as a new customer message. Review logs/app.log for bridge failures and amoCRM delivery status.
