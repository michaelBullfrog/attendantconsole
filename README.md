# Webex Calling Supervisor Dashboard

This FastAPI app gives supervisors a table-style dashboard for Webex Calling user status.

## Main URLs

- `/supervisor` - Supervisor dashboard UI
- `/api/agents` - JSON agent status feed
- `/api/events` - Recent webhook event list
- `/oauth/start` - Connect a Webex Calling user
- `/oauth/callback` - Webex OAuth redirect URI
- `/webex/calling-events` - Webex telephony_calls webhook target
- `/health` - Health check

## Render settings

Build Command:

```bash
pip install -r requirements.txt
```

Start Command:

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

## Render Environment Variables

```text
WEBEX_CLIENT_ID=your_client_id
WEBEX_CLIENT_SECRET=your_client_secret
WEBEX_REDIRECT_URI=https://your-render-service.onrender.com/oauth/callback
WEBEX_WEBHOOK_TARGET_URL=https://your-render-service.onrender.com/webex/calling-events
```

## Webex Integration Redirect URI

```text
https://your-render-service.onrender.com/oauth/callback
```

## How users are added

Each Webex Calling user must open:

```text
https://your-render-service.onrender.com/oauth/start
```

After authorization, the app stores their email/person ID and creates a user-level `telephony_calls` webhook.

## Status mapping

- `data.state = alerting` -> `Ringing`
- `data.state = connected` -> `On Call`
- webhook `event = deleted` -> `Not On Call`
