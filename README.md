# Webex Attendant Console

A FastAPI-based Webex Calling attendant console that tracks authenticated Webex users, displays live call status, supports call-control actions, manages DND, and can send completed call time to Rev.io PSA as a ticket.

---

## Overview

This application provides a browser-based attendant console for Webex Calling users. It uses Webex OAuth and `telephony_calls` webhooks to monitor call activity and update a live dashboard.

The console is designed for users who need to:

- View Webex Calling user status in near real time
- See whether users are ringing, on a call, outbound, available, or needing refresh
- Group users by organization
- Transfer their own active call to another user's extension
- Call another user directly from the console
- Turn DND on or off for users
- Reset a stuck dashboard status
- Track completed calls for authenticated users
- Send completed call details to Rev.io PSA as tickets

---

## Main URLs

| URL | Purpose |
|---|---|
| `/` | Connector landing page |
| `/attendantconsole` | Main attendant console dashboard |
| `/supervisor` | Redirects to `/attendantconsole` |
| `/oauth/start` | Starts Webex OAuth authorization |
| `/oauth/callback` | Webex OAuth redirect URI |
| `/oauth/remove/start` | Starts disconnect flow |
| `/oauth/remove/callback` | Disconnect redirect URI |
| `/webex/calling-events` | Webex `telephony_calls` webhook target |
| `/api/agents` | Returns dashboard user status data |
| `/api/events/clear` | Clears stored webhook/event activity |
| `/api/completed-calls` | Returns completed authenticated call logs |
| `/api/revio/debug` | Safe Rev.io PSA configuration/debug output |
| `/health` | Health and configuration check |

---

## Current Features

### Webex OAuth User Connection

Each Webex Calling user should authenticate through:

```text
/oauth/start
```

After authorization, the app:

1. Stores the user's Webex identity
2. Stores OAuth token information for call-control actions
3. Creates or reuses a Webex `telephony_calls` webhook
4. Adds the user to the attendant console
5. Redirects the user to:

```text
/attendantconsole?connected=1
```

---

### Live Attendant Console Dashboard

The main dashboard displays authenticated Webex users with:

- User email
- Organization
- Extension
- Status
- Time in state
- Display name
- Webex state
- Event type
- Remote party
- Remote number
- Transfer button
- Call button
- DND status and controls
- Reset status button

The dashboard supports:

- Search
- Organization filtering
- State filtering
- Refresh users
- Refresh extensions
- Refresh organizations
- Refresh DND
- Reset column order
- Horizontal table scrolling
- Drag-and-drop column ordering

---

### Status Values

The console displays these user-friendly statuses:

| Status | Meaning |
|---|---|
| `Ringing` | User has an incoming call alerting/ringing |
| `On Call` | User is connected or active on a call |
| `Outbound` | User placed an outbound call or Webex returned a state previously treated as unknown |
| `Not On Call` | User is not actively on a call |
| `Needs Refresh` | User has been stuck in an active state longer than the configured stale threshold |

The default stale threshold is 24 hours:

```text
STALE_STATUS_AFTER_SECONDS=86400
```

---

### Organization Grouping

Users are grouped visually by organization in the dashboard.

Example:

```text
Bullfrog Group, LLC
  user1@domain.com
  user2@domain.com

Customer ABC
  user1@customer.com
  user2@customer.com
```

---

### Hidden Unauthenticated Users

If a Webex webhook event is received for a user who has not authenticated through `/oauth/start`, the app does not display the raw Webex person ID in the main dashboard.

This prevents long Webex ID strings from cluttering the console.

Unauthenticated webhook-only users are automatically removed after the configured retention period:

```text
UNAUTHENTICATED_AGENT_RETENTION_SECONDS=86400
```

---

## Call Control Features

### Transfer My Active Call

The **Transfer** button attempts to transfer the currently signed-in console user's active call to the selected user's extension.

Important behavior:

- The person clicking Transfer must be the person currently on the call.
- This is not designed to transfer another user's call.
- The clicking user must have authenticated through `/oauth/start`.
- The destination user should have an extension available.

---

### Call User

The **Call** button attempts to place a Webex Calling call from the signed-in console user to the selected user's extension.

This is useful for quickly dialing another user from the attendant console.

---

### Reset Status

The **Reset** button manually resets a user's local dashboard status back to:

```text
Not On Call
```

This is useful if a missed webhook leaves a user stuck in:

```text
Needs Refresh
```

or another incorrect call state.

---

## DND Features

The console includes DND controls per user:

- DND status pill
- DND On button
- DND Off button

DND is applied through the Webex Calling user/person settings endpoint.

Default DND endpoint template:

```text
WEBEX_DND_ENDPOINT_TEMPLATE=https://webexapis.com/v1/people/{person_id}/features/doNotDisturb
```

Optional default ring reminder behavior:

```text
DND_DEFAULT_RING_REMINDER=false
```

DND may use:

1. `WEBEX_ADMIN_TOKEN`, if configured
2. Stored user OAuth token fallback, if available and permitted

---

## Completed Calls / Rev.io PSA Ticket Log

The dashboard includes a **Completed Calls / PSA Ticket Log** section.

When an authenticated user completes a call, the app logs:

- User
- Organization
- Remote party
- Remote number
- Call start time
- Call end time
- Total call duration
- Call ID
- Call session ID
- PSA ticket status

Each completed call has a **Send PSA** button.

---

## Rev.io PSA Ticket Creation

The app can send completed call details to Rev.io PSA using the Create Ticket API.

The ticket payload includes required fields:

```json
{
  "ticketDescription": "string",
  "ticketTypeId": 1,
  "ticketStatusId": 1,
  "ticketPriorityId": 1
}
```

The app also sends call details in `workRequested`.

Example included details:

- Webex user
- Organization
- Remote party
- Remote number
- Call duration
- Call start and end time
- Call ID
- Call session ID

---

## Rev.io PSA Authentication Flow

The current working Rev.io PSA flow uses:

1. `REVIO_PSA_API_KEY`
2. Exchange API key for JWT
3. Use JWT as Bearer token
4. Send `X-Revio-Host`

The app exchanges the API key at:

```text
POST https://api.psarev.io/api/v1/auth/api-key/exchange
```

Then creates tickets at:

```text
POST https://api.psarev.io/psac/api/v1/ticket
```

Ticket requests use headers similar to:

```text
Authorization: Bearer <JWT from exchange>
X-Revio-Host: bullfrog.psarev.io
Content-Type: application/json
```

---

## Memory and Data Management

To help keep memory and storage usage low, the app supports:

- Automatic event cleanup
- Event row limit
- Raw webhook payload disabling
- Hidden unauthenticated user cleanup
- Short completed-call/event retention behavior

Recommended low-memory settings:

```text
AUTO_CLEAR_EVENTS_ENABLED=true
AUTO_CLEAR_EVENTS_EVERY_SECONDS=1800
AUTO_CLEAR_EVENTS_VACUUM=false
MAX_EVENT_ROWS=500
WEBHOOK_PAYLOAD_MAX_CHARS=1000
STORE_RAW_WEBHOOK_PAYLOADS=false
UNAUTHENTICATED_AGENT_RETENTION_SECONDS=86400
```

For even lower storage usage:

```text
WEBHOOK_PAYLOAD_MAX_CHARS=0
MAX_EVENT_ROWS=100
AUTO_CLEAR_EVENTS_EVERY_SECONDS=900
```

---

## Refresh Rates

The dashboard uses the faster refresh behavior:

```text
Users refresh: every 3 seconds
Completed calls refresh: every 5 seconds
Timer text refresh: every 1 second
```

The table is not fully rebuilt every second. The one-second refresh updates visible timer text only, which helps prevent buttons and controls from disappearing inside embedded Webex iframes.

---

## Required Python Packages

`requirements.txt`:

```text
fastapi
uvicorn
requests
python-dotenv
```

---

## Render Deployment

### Build Command

```bash
pip install -r requirements.txt
```

### Start Command

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

Make sure the Python file is named:

```text
main.py
```

---

## Required Webex Environment Variables

```text
WEBEX_CLIENT_ID=your_webex_client_id
WEBEX_CLIENT_SECRET=your_webex_client_secret
WEBEX_REDIRECT_URI=https://your-render-service.onrender.com/oauth/callback
WEBEX_WEBHOOK_TARGET_URL=https://your-render-service.onrender.com/webex/calling-events
```

Optional:

```text
WEBEX_ADMIN_TOKEN=your_webex_admin_token
WEBEX_ORG_NAME_MAP={"orgId":"Friendly Org Name"}
WEBEX_DND_ENDPOINT_TEMPLATE=https://webexapis.com/v1/people/{person_id}/features/doNotDisturb
DND_DEFAULT_RING_REMINDER=false
```

---

## Webex Integration Redirect URI

In the Webex Developer Portal, add this redirect URI:

```text
https://your-render-service.onrender.com/oauth/callback
```

If using the disconnect flow, also add:

```text
https://your-render-service.onrender.com/oauth/remove/callback
```

---

## Webex Scopes

The app may require scopes similar to:

```text
spark:calls_read
spark:calls_write
spark:webhooks_write
spark:webhooks_read
spark:people_read
spark-admin:organizations_read
spark-admin:people_read
```

Notes:

- `spark:calls_read` is used for call visibility.
- `spark:calls_write` is used for call-control actions such as transfer/call.
- Admin scopes may be needed for org/user enrichment.
- DND changes may require admin permissions or valid user permissions.

---

## Required Rev.io PSA Environment Variables

Current working Rev.io PSA variables:

```text
REVIO_PSA_BASE_URL=https://api.psarev.io
REVIO_PSA_API_KEY=your_revio_psa_api_key
REVIO_PSA_HOST=bullfrog.psarev.io
REVIO_PSA_TICKET_TYPE_ID=1
REVIO_PSA_TICKET_STATUS_ID=1
REVIO_PSA_TICKET_PRIORITY_ID=1
REVIO_INCLUDE_CUSTOM_FIELDS=false
```

Optional:

```text
REVIO_TICKET_AUTO_CREATE_ON_CALL_END=false
REVIO_TICKET_CUSTOMER_ID=
REVIO_TICKET_SEVERITY_ID=
REVIO_TICKET_USER_GROUP_TARGET=
```

Older test variables that should generally be cleared to avoid confusion:

```text
REVIO_PSA_AUTH_HEADER
REVIO_PSA_BEARER_TOKEN
REVIO_PSA_API_KEY_HEADER
REVIO_PSA_API_KEY_SCHEME
REVIO_TICKET_TYPE_ID
REVIO_TICKET_STATUS_ID
REVIO_TICKET_PRIORITY_ID
```

---

## Recommended Render Environment Variables

```text
WEBEX_CLIENT_ID=your_webex_client_id
WEBEX_CLIENT_SECRET=your_webex_client_secret
WEBEX_REDIRECT_URI=https://your-render-service.onrender.com/oauth/callback
WEBEX_WEBHOOK_TARGET_URL=https://your-render-service.onrender.com/webex/calling-events

AUTO_CLEAR_EVENTS_ENABLED=true
AUTO_CLEAR_EVENTS_EVERY_SECONDS=1800
AUTO_CLEAR_EVENTS_VACUUM=false
MAX_EVENT_ROWS=500
WEBHOOK_PAYLOAD_MAX_CHARS=1000
STORE_RAW_WEBHOOK_PAYLOADS=false
UNAUTHENTICATED_AGENT_RETENTION_SECONDS=86400
STALE_STATUS_AFTER_SECONDS=86400

REVIO_PSA_BASE_URL=https://api.psarev.io
REVIO_PSA_API_KEY=your_revio_psa_api_key
REVIO_PSA_HOST=bullfrog.psarev.io
REVIO_PSA_TICKET_TYPE_ID=1
REVIO_PSA_TICKET_STATUS_ID=1
REVIO_PSA_TICKET_PRIORITY_ID=1
REVIO_INCLUDE_CUSTOM_FIELDS=false
```

---

## Useful Health Checks

### App Health

```text
/health
```

Confirms important settings such as:

- Webex client ID configured
- Redirect URI configured
- Webhook target configured
- Admin token present or missing
- Event cleanup settings
- Rev.io PSA configuration
- DND endpoint settings

### Rev.io Debug

```text
/api/revio/debug
```

Shows safe Rev.io configuration details without exposing secrets.

Useful fields include:

- Final ticket URL
- Auth mode
- JWT exchange status
- Whether `X-Revio-Host` is attached
- Ticket type/status/priority IDs
- Last PSA send error

---

## Manual Maintenance Endpoints

### Clear Webhook/Event History

```text
POST /api/events/clear
```

PowerShell:

```powershell
Invoke-RestMethod -Method POST "https://your-render-service.onrender.com/api/events/clear"
```

This clears stored webhook activity only. It does not remove users, OAuth sessions, or agent rows.

---

## Known Limitations

### Dashboard Access

Webex OAuth connects users and enables actions, but it is not the same as protecting the dashboard page itself.

For production, consider adding a dedicated dashboard login or access control layer.

### Token Storage

OAuth token data is stored so call-control actions can work.

For production, consider:

- Encrypting stored tokens
- Using a managed database
- Restricting database access
- Adding token cleanup policies

### Transfer Scope

Transfer is designed as:

```text
Transfer my active call
```

It is not designed as:

```text
Transfer another user's active call
```

The user clicking the Transfer button must be the user currently on the call.

### Rev.io Tickets

Completed calls are sent to Rev.io only when clicking **Send PSA**, unless auto-create is explicitly enabled.

By default:

```text
REVIO_TICKET_AUTO_CREATE_ON_CALL_END=false
```

### Webhook Dependence

The dashboard depends on Webex webhook delivery. If a webhook is missed, a user can appear stuck in a state until:

- Another webhook arrives
- The user is reset manually
- The status becomes `Needs Refresh`

---

## Troubleshooting

### Webex OAuth `invalid_scope`

If Webex returns:

```text
invalid_scope
```

Verify the scopes are selected/enabled in the Webex Developer Portal integration.

---

### Long Webex ID Appears

Users who do not authenticate may appear only as webhook IDs. The app hides unauthenticated users from the main table, but users should still authenticate through:

```text
/oauth/start
```

---

### Transfer or Call Fails

Confirm:

- User has authenticated through `/oauth/start`
- User has a valid session cookie
- Webex scopes include call write permissions
- User has an active call for transfer
- Destination user has an extension

---

### DND Fails

Confirm:

- User exists and has Webex Calling
- DND endpoint is correct
- `WEBEX_ADMIN_TOKEN` is configured if changing other users
- The signed-in user's token has enough permission if relying on fallback

---

### Rev.io PSA Fails

Check:

```text
/api/revio/debug
```

Confirm:

- `REVIO_PSA_BASE_URL=https://api.psarev.io`
- `REVIO_PSA_API_KEY` is set
- `REVIO_PSA_HOST=bullfrog.psarev.io`
- JWT exchange succeeds
- Ticket type/status/priority IDs are valid
- The API key has permission to create tickets

---

## Project File Structure

Typical repo:

```text
.
├── main.py
├── requirements.txt
└── README.md
```

Optional runtime data:

```text
data/
└── attendant_console.db
```

---

## Security Notes

Do not commit secrets to GitHub.

Keep these only in Render environment variables:

- `WEBEX_CLIENT_SECRET`
- `WEBEX_ADMIN_TOKEN`
- `REVIO_PSA_API_KEY`
- Any OAuth tokens or API credentials

If a secret is accidentally exposed, rotate it immediately.

---

## Current Summary

This app is a custom Webex Calling Attendant Console that:

- Tracks authenticated Webex Calling users
- Shows live call status
- Groups users by organization
- Supports transfer of the signed-in user's active call
- Supports direct call-to-user actions
- Supports DND on/off controls
- Resets stuck user statuses
- Logs completed authenticated calls
- Sends completed call time to Rev.io PSA tickets
- Uses Rev.io API key exchange to JWT
- Includes low-memory cleanup settings for Render hosting
