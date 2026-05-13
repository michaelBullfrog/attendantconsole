import os
import json
import requests
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

app = FastAPI()

WEBEX_CLIENT_ID = os.getenv("WEBEX_CLIENT_ID")
WEBEX_CLIENT_SECRET = os.getenv("WEBEX_CLIENT_SECRET")
WEBEX_REDIRECT_URI = os.getenv("WEBEX_REDIRECT_URI")
WEBEX_WEBHOOK_TARGET_URL = os.getenv("WEBEX_WEBHOOK_TARGET_URL")

SCOPES = "spark:calls_read spark:webhooks_write spark:webhooks_read spark:people_read"

# In-memory status store.
# Good for testing. For production, move this to a database.
USER_STATUS: Dict[str, Dict[str, Any]] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_get(data: Dict[str, Any], *keys, default=None):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return current if current is not None else default


def classify_call_status(event: Dict[str, Any]) -> str:
    """
    Attempts to classify the user status from Webex telephony_calls webhook payloads.
    Webex payload shapes can vary, so this intentionally checks multiple possible fields.
    """
    event_type = str(event.get("event", "")).lower()
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}

    # Common possible state fields
    raw_state = (
        data.get("state")
        or data.get("status")
        or data.get("callState")
        or data.get("callStatus")
        or data.get("eventType")
        or ""
    )

    state = str(raw_state).lower()

    if event_type == "deleted":
        return "Not On Call"

    if any(word in state for word in ["ring", "alert", "incoming", "offered"]):
        return "Ringing"

    if any(word in state for word in ["connected", "active", "held", "hold", "remoteheld", "bridged"]):
        return "On Call"

    if event_type in ["created", "updated"]:
        # Conservative default: if Webex created/updated a telephony call, the user is probably busy.
        return "On Call"

    return "Unknown"


def extract_user_key(event: Dict[str, Any]) -> str:
    """
    Tries to identify the user from the webhook.
    If the event does not include an email/personId, this falls back to webhook id.
    """
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}

    return (
        data.get("personEmail")
        or data.get("ownerEmail")
        or data.get("email")
        or data.get("personId")
        or data.get("ownerId")
        or event.get("ownedBy")
        or event.get("createdBy")
        or event.get("webhookId")
        or "unknown-user"
    )


def extract_call_id(event: Dict[str, Any]) -> Optional[str]:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    return (
        data.get("callId")
        or data.get("id")
        or data.get("callSessionId")
        or data.get("correlationId")
    )


def create_call_status_webhook(user_access_token: str):
    if not WEBEX_WEBHOOK_TARGET_URL:
        raise HTTPException(status_code=500, detail="Missing WEBEX_WEBHOOK_TARGET_URL")

    url = "https://webexapis.com/v1/webhooks"

    payload = {
        "name": "User Webex Calling Status",
        "targetUrl": WEBEX_WEBHOOK_TARGET_URL,
        "resource": "telephony_calls",
        "event": "all"
    }

    headers = {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers, timeout=20)

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


def get_me(user_access_token: str):
    url = "https://webexapis.com/v1/people/me"
    headers = {"Authorization": f"Bearer {user_access_token}"}
    response = requests.get(url, headers=headers, timeout=20)

    if response.status_code >= 400:
        return None

    return response.json()


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(
        """
<!DOCTYPE html>
<html>
<head>
    <title>Webex Calling Attendant Console</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
        :root {
            --bg: #f4f7fb;
            --card: #ffffff;
            --text: #17202a;
            --muted: #6b7280;
            --border: #dce3eb;
            --green: #14a44d;
            --yellow: #f5b301;
            --red: #d93025;
            --blue: #2563eb;
            --gray: #64748b;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: Arial, Helvetica, sans-serif;
            background: var(--bg);
            color: var(--text);
        }

        header {
            background: #0f172a;
            color: white;
            padding: 22px 28px;
        }

        header h1 {
            margin: 0;
            font-size: 24px;
        }

        header p {
            margin: 6px 0 0;
            color: #cbd5e1;
        }

        main {
            padding: 24px;
            max-width: 1200px;
            margin: 0 auto;
        }

        .toolbar {
            display: flex;
            gap: 12px;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 18px;
            flex-wrap: wrap;
        }

        .toolbar-left {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }

        input {
            padding: 11px 13px;
            border: 1px solid var(--border);
            border-radius: 10px;
            min-width: 260px;
            font-size: 14px;
        }

        button, a.button {
            padding: 11px 14px;
            border: 0;
            border-radius: 10px;
            background: var(--blue);
            color: white;
            cursor: pointer;
            font-size: 14px;
            text-decoration: none;
            display: inline-block;
        }

        button.secondary {
            background: #334155;
        }

        .summary {
            display: grid;
            grid-template-columns: repeat(4, minmax(130px, 1fr));
            gap: 14px;
            margin-bottom: 18px;
        }

        .summary-card {
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 16px;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.06);
        }

        .summary-card .label {
            color: var(--muted);
            font-size: 13px;
        }

        .summary-card .value {
            font-size: 28px;
            font-weight: 700;
            margin-top: 6px;
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(265px, 1fr));
            gap: 16px;
        }

        .agent-card {
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 18px;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.06);
        }

        .agent-top {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            align-items: flex-start;
        }

        .agent-name {
            font-weight: 700;
            font-size: 17px;
            word-break: break-word;
        }

        .agent-meta {
            color: var(--muted);
            font-size: 13px;
            margin-top: 5px;
            word-break: break-word;
        }

        .badge {
            padding: 6px 10px;
            border-radius: 999px;
            color: white;
            font-size: 12px;
            font-weight: 700;
            white-space: nowrap;
        }

        .status-on-call { background: var(--red); }
        .status-ringing { background: var(--yellow); color: #111827; }
        .status-not-on-call { background: var(--green); }
        .status-unknown { background: var(--gray); }

        .details {
            margin-top: 14px;
            color: var(--muted);
            font-size: 13px;
            line-height: 1.55;
        }

        .empty {
            background: var(--card);
            border: 1px dashed var(--border);
            border-radius: 18px;
            padding: 30px;
            text-align: center;
            color: var(--muted);
        }

        .footer-note {
            margin-top: 18px;
            color: var(--muted);
            font-size: 13px;
        }

        @media (max-width: 720px) {
            .summary {
                grid-template-columns: repeat(2, minmax(130px, 1fr));
            }

            input {
                min-width: 100%;
            }

            .toolbar {
                align-items: stretch;
            }
        }
    </style>
</head>
<body>
    <header>
        <h1>Webex Calling Attendant Console</h1>
        <p>Live user call status from Webex Calling webhook events</p>
    </header>

    <main>
        <div class="toolbar">
            <div class="toolbar-left">
                <input id="search" placeholder="Search user, email, status..." oninput="renderAgents()" />
                <button onclick="loadData()">Refresh</button>
                <button class="secondary" onclick="clearLocalFilter()">Clear Search</button>
            </div>
            <a class="button" href="/oauth/start">Connect Webex User</a>
        </div>

        <section class="summary">
            <div class="summary-card">
                <div class="label">Total Users</div>
                <div class="value" id="totalUsers">0</div>
            </div>
            <div class="summary-card">
                <div class="label">On Call</div>
                <div class="value" id="onCallUsers">0</div>
            </div>
            <div class="summary-card">
                <div class="label">Ringing</div>
                <div class="value" id="ringingUsers">0</div>
            </div>
            <div class="summary-card">
                <div class="label">Not On Call</div>
                <div class="value" id="availableUsers">0</div>
            </div>
        </section>

        <section id="agentGrid" class="grid"></section>

        <div class="footer-note">
            Auto-refreshes every 5 seconds. This test version stores status in memory, so statuses reset when Render restarts.
        </div>
    </main>

    <script>
        let agents = [];

        function statusClass(status) {
            const clean = (status || "Unknown").toLowerCase();
            if (clean === "on call") return "status-on-call";
            if (clean === "ringing") return "status-ringing";
            if (clean === "not on call") return "status-not-on-call";
            return "status-unknown";
        }

        function formatDate(value) {
            if (!value) return "N/A";
            try {
                return new Date(value).toLocaleString();
            } catch {
                return value;
            }
        }

        async function loadData() {
            try {
                const response = await fetch("/api/status");
                const data = await response.json();
                agents = data.users || [];
                renderAgents();
            } catch (err) {
                console.error("Failed to load status", err);
            }
        }

        function renderAgents() {
            const search = document.getElementById("search").value.toLowerCase().trim();

            const filtered = agents.filter(agent => {
                const haystack = JSON.stringify(agent).toLowerCase();
                return haystack.includes(search);
            });

            document.getElementById("totalUsers").textContent = agents.length;
            document.getElementById("onCallUsers").textContent = agents.filter(a => a.status === "On Call").length;
            document.getElementById("ringingUsers").textContent = agents.filter(a => a.status === "Ringing").length;
            document.getElementById("availableUsers").textContent = agents.filter(a => a.status === "Not On Call").length;

            const grid = document.getElementById("agentGrid");

            if (filtered.length === 0) {
                grid.innerHTML = `<div class="empty">No user status records yet. Connect a Webex user, then make or receive a Webex Calling call.</div>`;
                return;
            }

            grid.innerHTML = filtered.map(agent => `
                <article class="agent-card">
                    <div class="agent-top">
                        <div>
                            <div class="agent-name">${agent.displayName || agent.email || agent.userKey || "Unknown User"}</div>
                            <div class="agent-meta">${agent.email || agent.userKey || ""}</div>
                        </div>
                        <span class="badge ${statusClass(agent.status)}">${agent.status || "Unknown"}</span>
                    </div>
                    <div class="details">
                        <div><strong>Last Event:</strong> ${agent.lastEvent || "N/A"}</div>
                        <div><strong>Call ID:</strong> ${agent.callId || "N/A"}</div>
                        <div><strong>Updated:</strong> ${formatDate(agent.updatedAt)}</div>
                    </div>
                </article>
            `).join("");
        }

        function clearLocalFilter() {
            document.getElementById("search").value = "";
            renderAgents();
        }

        loadData();
        setInterval(loadData, 5000);
    </script>
</body>
</html>
        """
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/status")
def api_status():
    users = list(USER_STATUS.values())
    users.sort(key=lambda x: x.get("displayName") or x.get("email") or x.get("userKey") or "")
    return {"users": users, "count": len(users)}


@app.get("/api/status/raw")
def api_status_raw():
    return USER_STATUS


@app.post("/api/status/reset")
def reset_status():
    USER_STATUS.clear()
    return {"message": "Status store cleared"}


@app.get("/oauth/start")
def oauth_start():
    if not WEBEX_CLIENT_ID or not WEBEX_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing WEBEX_CLIENT_ID or WEBEX_REDIRECT_URI")

    auth_url = (
        "https://webexapis.com/v1/authorize"
        f"?client_id={WEBEX_CLIENT_ID}"
        "&response_type=code"
        f"&redirect_uri={WEBEX_REDIRECT_URI}"
        f"&scope={SCOPES.replace(' ', '%20')}"
        "&state=webex-calling-status"
    )

    return RedirectResponse(auth_url)


@app.get("/oauth/callback")
def oauth_callback(request: Request):
    code = request.query_params.get("code")

    if not code:
        raise HTTPException(status_code=400, detail="Missing authorization code")

    if not WEBEX_CLIENT_ID or not WEBEX_CLIENT_SECRET or not WEBEX_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing Webex OAuth environment variables")

    token_url = "https://webexapis.com/v1/access_token"

    payload = {
        "grant_type": "authorization_code",
        "client_id": WEBEX_CLIENT_ID,
        "client_secret": WEBEX_CLIENT_SECRET,
        "code": code,
        "redirect_uri": WEBEX_REDIRECT_URI,
    }

    response = requests.post(token_url, data=payload, timeout=20)

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    tokens = response.json()
    access_token = tokens.get("access_token")

    me = get_me(access_token)
    webhook = create_call_status_webhook(access_token)

    if me:
        user_key = me.get("emails", ["unknown-user"])[0] if me.get("emails") else me.get("id", "unknown-user")
        USER_STATUS[user_key] = {
            "userKey": user_key,
            "personId": me.get("id"),
            "displayName": me.get("displayName"),
            "email": user_key,
            "status": "Not On Call",
            "lastEvent": "Connected",
            "callId": None,
            "updatedAt": now_iso(),
            "webhookId": webhook.get("id"),
        }

    return HTMLResponse(
        f"""
        <html>
            <head>
                <title>Webex Connected</title>
                <meta http-equiv="refresh" content="3; url=/" />
                <style>
                    body {{ font-family: Arial, sans-serif; padding: 40px; background: #f4f7fb; }}
                    .card {{ background: white; padding: 24px; border-radius: 16px; max-width: 650px; box-shadow: 0 8px 20px rgba(15,23,42,.08); }}
                    a {{ color: #2563eb; }}
                </style>
            </head>
            <body>
                <div class="card">
                    <h2>Webex authorization successful</h2>
                    <p>Calling webhook created.</p>
                    <p><strong>Webhook ID:</strong> {webhook.get("id")}</p>
                    <p>Redirecting back to the dashboard...</p>
                    <p><a href="/">Go to dashboard now</a></p>
                </div>
            </body>
        </html>
        """
    )


@app.post("/webex/calling-events")
async def calling_events(request: Request):
    event = await request.json()

    print("Received Webex Calling event:")
    print(json.dumps(event, indent=2))

    user_key = extract_user_key(event)
    call_id = extract_call_id(event)
    status = classify_call_status(event)

    existing = USER_STATUS.get(user_key, {})

    USER_STATUS[user_key] = {
        "userKey": user_key,
        "personId": existing.get("personId"),
        "displayName": existing.get("displayName") or user_key,
        "email": existing.get("email") or user_key,
        "status": status,
        "lastEvent": event.get("event", "unknown"),
        "callId": None if status == "Not On Call" else call_id,
        "updatedAt": now_iso(),
        "webhookId": event.get("webhookId") or existing.get("webhookId"),
    }

    return {"status": "received"}
