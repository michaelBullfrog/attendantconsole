import os
import json
import requests
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI()

WEBEX_CLIENT_ID = os.getenv("WEBEX_CLIENT_ID")
WEBEX_CLIENT_SECRET = os.getenv("WEBEX_CLIENT_SECRET")
WEBEX_REDIRECT_URI = os.getenv("WEBEX_REDIRECT_URI")
WEBEX_WEBHOOK_TARGET_URL = os.getenv("WEBEX_WEBHOOK_TARGET_URL")

SCOPES = "spark:calls_read spark:webhooks_write spark:webhooks_read spark:people_read"

# In-memory test storage.
# NOTE: This resets when Render restarts/redeploys.
USERS: Dict[str, Dict[str, Any]] = {}
ACTIVE_CALLS: Dict[str, Dict[str, Any]] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def classify_status(event: Dict[str, Any]) -> str:
    """
    Webex Calling telephony_calls examples:
      - data.state = alerting   => Ringing
      - data.state = connected  => On Call
      - webhook event = deleted => Not On Call
    """
    webhook_event = str(event.get("event", "")).lower()
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}

    state = str(data.get("state", "")).lower()
    event_type = str(data.get("eventType", "")).lower()

    if webhook_event == "deleted" or event_type in {"ended", "released", "disconnected"}:
        return "Not On Call"

    if state in {"alerting", "ringing"} or event_type in {"received", "offered"}:
        return "Ringing"

    if state in {
        "connected",
        "held",
        "remoteheld",
        "active",
        "bridged",
        "consulting",
        "conference",
    } or event_type in {"answered", "connected"}:
        return "On Call"

    return "Unknown"


def extract_call_id(event: Dict[str, Any]) -> Optional[str]:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    return data.get("callId") or data.get("id")


def extract_call_session_id(event: Dict[str, Any]) -> Optional[str]:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    return data.get("callSessionId")


def extract_remote_party(event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    remote = data.get("remoteParty", {})
    return remote if isinstance(remote, dict) else {}


def extract_user_key(event: Dict[str, Any]) -> str:
    """
    In your sample payload, the webhook has actorId and createdBy as the person ID.
    Because telephony_calls webhooks are user-level, actorId is the safest key here.
    """
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}

    return (
        data.get("personEmail")
        or data.get("ownerEmail")
        or data.get("email")
        or data.get("personId")
        or event.get("actorId")
        or event.get("createdBy")
        or event.get("webhookId")
        or "unknown-user"
    )


def get_me(user_access_token: str) -> Optional[Dict[str, Any]]:
    url = "https://webexapis.com/v1/people/me"
    headers = {"Authorization": f"Bearer {user_access_token}"}
    response = requests.get(url, headers=headers, timeout=20)

    if response.status_code >= 400:
        print("Unable to get /people/me:", response.text)
        return None

    return response.json()


def create_call_status_webhook(user_access_token: str) -> Dict[str, Any]:
    if not WEBEX_WEBHOOK_TARGET_URL:
        raise HTTPException(status_code=500, detail="Missing WEBEX_WEBHOOK_TARGET_URL")

    url = "https://webexapis.com/v1/webhooks"

    payload = {
        "name": "User Webex Calling Status",
        "targetUrl": WEBEX_WEBHOOK_TARGET_URL,
        "resource": "telephony_calls",
        "event": "all",
    }

    headers = {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, json=payload, headers=headers, timeout=20)

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Webex Calling Attendant Console</title>
    <style>
        :root {
            --bg: #f5f7fb;
            --card: #ffffff;
            --border: #dbe3ef;
            --text: #111827;
            --muted: #667085;
            --blue: #2563eb;
            --dark: #0f172a;
            --green: #16a34a;
            --red: #dc2626;
            --amber: #f59e0b;
            --gray: #64748b;
        }

        * { box-sizing: border-box; }

        body {
            margin: 0;
            font-family: Arial, Helvetica, sans-serif;
            background: var(--bg);
            color: var(--text);
        }

        header {
            background: linear-gradient(135deg, #0f172a, #1e293b);
            color: white;
            padding: 26px 30px;
        }

        header h1 {
            margin: 0;
            font-size: 26px;
        }

        header p {
            margin: 7px 0 0;
            color: #cbd5e1;
            font-size: 14px;
        }

        main {
            max-width: 1250px;
            margin: 0 auto;
            padding: 24px;
        }

        .toolbar {
            display: flex;
            justify-content: space-between;
            gap: 14px;
            align-items: center;
            flex-wrap: wrap;
            margin-bottom: 18px;
        }

        .toolbar-left {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            align-items: center;
        }

        input, select {
            border: 1px solid var(--border);
            background: white;
            border-radius: 10px;
            padding: 11px 13px;
            font-size: 14px;
            min-width: 240px;
        }

        button, .button {
            border: 0;
            border-radius: 10px;
            background: var(--blue);
            color: white;
            padding: 11px 15px;
            font-size: 14px;
            cursor: pointer;
            text-decoration: none;
            display: inline-block;
        }

        button.secondary {
            background: #334155;
        }

        .summary {
            display: grid;
            grid-template-columns: repeat(4, minmax(150px, 1fr));
            gap: 14px;
            margin-bottom: 18px;
        }

        .summary-card {
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 16px;
            box-shadow: 0 8px 22px rgba(15, 23, 42, 0.06);
        }

        .summary-card .label {
            color: var(--muted);
            font-size: 13px;
        }

        .summary-card .value {
            font-size: 30px;
            font-weight: 800;
            margin-top: 6px;
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(285px, 1fr));
            gap: 16px;
        }

        .card {
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 18px;
            box-shadow: 0 8px 22px rgba(15, 23, 42, 0.06);
        }

        .top {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            align-items: flex-start;
        }

        .name {
            font-weight: 800;
            font-size: 17px;
            word-break: break-word;
        }

        .email {
            margin-top: 4px;
            color: var(--muted);
            font-size: 13px;
            word-break: break-word;
        }

        .badge {
            border-radius: 999px;
            padding: 7px 11px;
            font-weight: 800;
            font-size: 12px;
            white-space: nowrap;
        }

        .status-on-call {
            background: #fee2e2;
            color: var(--red);
            border: 1px solid #fecaca;
        }

        .status-ringing {
            background: #fef3c7;
            color: #92400e;
            border: 1px solid #fde68a;
        }

        .status-not-on-call {
            background: #dcfce7;
            color: var(--green);
            border: 1px solid #bbf7d0;
        }

        .status-unknown {
            background: #e2e8f0;
            color: var(--gray);
            border: 1px solid #cbd5e1;
        }

        .details {
            margin-top: 15px;
            color: var(--muted);
            line-height: 1.6;
            font-size: 13px;
        }

        .details strong {
            color: #334155;
        }

        .empty {
            grid-column: 1 / -1;
            background: white;
            border: 1px dashed var(--border);
            border-radius: 18px;
            padding: 34px;
            color: var(--muted);
            text-align: center;
        }

        .note {
            margin-top: 18px;
            color: var(--muted);
            font-size: 13px;
        }

        @media (max-width: 760px) {
            main { padding: 16px; }
            .summary { grid-template-columns: repeat(2, 1fr); }
            input, select { min-width: 100%; }
            .toolbar, .toolbar-left { align-items: stretch; width: 100%; }
            button, .button { width: 100%; text-align: center; }
        }
    </style>
</head>
<body>
    <header>
        <h1>Webex Calling Attendant Console</h1>
        <p>Tracks user status from Webex Calling telephony call webhook events.</p>
    </header>

    <main>
        <div class="toolbar">
            <div class="toolbar-left">
                <input id="search" placeholder="Search users or numbers..." oninput="render()" />
                <select id="statusFilter" onchange="render()">
                    <option value="All">All Statuses</option>
                    <option value="Ringing">Ringing</option>
                    <option value="On Call">On Call</option>
                    <option value="Not On Call">Not On Call</option>
                    <option value="Unknown">Unknown</option>
                </select>
                <button onclick="loadData()">Refresh</button>
                <button class="secondary" onclick="resetStatuses()">Reset</button>
            </div>
            <a class="button" href="/oauth/start">Connect Webex User</a>
        </div>

        <section class="summary">
            <div class="summary-card">
                <div class="label">Total Users</div>
                <div class="value" id="total">0</div>
            </div>
            <div class="summary-card">
                <div class="label">Ringing</div>
                <div class="value" id="ringing">0</div>
            </div>
            <div class="summary-card">
                <div class="label">On Call</div>
                <div class="value" id="oncall">0</div>
            </div>
            <div class="summary-card">
                <div class="label">Not On Call</div>
                <div class="value" id="available">0</div>
            </div>
        </section>

        <section class="grid" id="userGrid"></section>

        <div class="note">
            Auto-refreshes every 3 seconds. This version stores status in memory, so records reset when Render restarts or redeploys.
        </div>
    </main>

<script>
let users = [];

function badgeClass(status) {
    if (status === "On Call") return "status-on-call";
    if (status === "Ringing") return "status-ringing";
    if (status === "Not On Call") return "status-not-on-call";
    return "status-unknown";
}

function fmtDate(value) {
    if (!value) return "N/A";
    try {
        return new Date(value).toLocaleString();
    } catch {
        return value;
    }
}

async function loadData() {
    const res = await fetch("/api/status");
    const data = await res.json();
    users = data.users || [];
    render();
}

function render() {
    const search = document.getElementById("search").value.toLowerCase().trim();
    const filter = document.getElementById("statusFilter").value;

    document.getElementById("total").textContent = users.length;
    document.getElementById("ringing").textContent = users.filter(u => u.status === "Ringing").length;
    document.getElementById("oncall").textContent = users.filter(u => u.status === "On Call").length;
    document.getElementById("available").textContent = users.filter(u => u.status === "Not On Call").length;

    const filtered = users.filter(u => {
        const matchesStatus = filter === "All" || u.status === filter;
        const blob = JSON.stringify(u).toLowerCase();
        return matchesStatus && blob.includes(search);
    });

    const grid = document.getElementById("userGrid");

    if (!filtered.length) {
        grid.innerHTML = `
            <div class="empty">
                No users to display yet. Connect a Webex Calling user, then make or receive a test call.
            </div>
        `;
        return;
    }

    grid.innerHTML = filtered.map(u => `
        <article class="card">
            <div class="top">
                <div>
                    <div class="name">${u.displayName || u.email || u.userKey || "Unknown User"}</div>
                    <div class="email">${u.email || u.personId || u.userKey || ""}</div>
                </div>
                <span class="badge ${badgeClass(u.status)}">${u.status || "Unknown"}</span>
            </div>

            <div class="details">
                <div><strong>Remote Party:</strong> ${u.remoteName || "N/A"}</div>
                <div><strong>Remote Number:</strong> ${u.remoteNumber || "N/A"}</div>
                <div><strong>Call Type:</strong> ${u.remoteCallType || "N/A"}</div>
                <div><strong>Webex State:</strong> ${u.webexState || "N/A"}</div>
                <div><strong>Event Type:</strong> ${u.eventType || "N/A"}</div>
                <div><strong>Call Session:</strong> ${u.callSessionId || "N/A"}</div>
                <div><strong>Updated:</strong> ${fmtDate(u.updatedAt)}</div>
            </div>
        </article>
    `).join("");
}

async function resetStatuses() {
    await fetch("/api/status/reset", { method: "POST" });
    await loadData();
}

loadData();
setInterval(loadData, 3000);
</script>
</body>
</html>
    """


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/status")
def api_status():
    output = list(USERS.values())
    output.sort(key=lambda u: (u.get("status") != "Ringing", u.get("status") != "On Call", u.get("displayName") or ""))
    return {"count": len(output), "users": output}


@app.get("/api/status/raw")
def api_status_raw():
    return {"users": USERS, "activeCalls": ACTIVE_CALLS}


@app.post("/api/status/reset")
def api_status_reset():
    USERS.clear()
    ACTIVE_CALLS.clear()
    return {"message": "reset complete"}


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
        emails = me.get("emails") or []
        email = emails[0] if emails else None
        person_id = me.get("id")
        user_key = person_id or email or webhook.get("id") or "unknown-user"

        USERS[user_key] = {
            "userKey": user_key,
            "personId": person_id,
            "displayName": me.get("displayName") or email or user_key,
            "email": email,
            "status": "Not On Call",
            "webexState": None,
            "eventType": "connected",
            "callId": None,
            "callSessionId": None,
            "remoteName": None,
            "remoteNumber": None,
            "remoteCallType": None,
            "updatedAt": now_iso(),
            "webhookId": webhook.get("id"),
        }

    return HTMLResponse(f"""
        <html>
            <head>
                <title>Webex Connected</title>
                <meta http-equiv="refresh" content="3; url=/" />
                <style>
                    body {{ font-family: Arial, sans-serif; background: #f5f7fb; padding: 40px; }}
                    .card {{ background: white; border-radius: 16px; padding: 24px; max-width: 680px; box-shadow: 0 8px 22px rgba(15,23,42,.08); }}
                    a {{ color: #2563eb; }}
                </style>
            </head>
            <body>
                <div class="card">
                    <h2>Webex user connected successfully</h2>
                    <p>The user-level <strong>telephony_calls</strong> webhook was created.</p>
                    <p><strong>Webhook ID:</strong> {webhook.get("id")}</p>
                    <p>You will be redirected to the dashboard.</p>
                    <p><a href="/">Go to dashboard now</a></p>
                </div>
            </body>
        </html>
    """)


@app.post("/webex/calling-events")
async def calling_events(request: Request):
    event = await request.json()

    print("Received Webex Calling event:")
    print(json.dumps(event, indent=2))

    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    remote = extract_remote_party(event)

    user_key = extract_user_key(event)
    status = classify_status(event)
    call_id = extract_call_id(event)
    call_session_id = extract_call_session_id(event)

    # Track call state by call ID as well, useful for debugging and future multi-call logic.
    if call_id:
        if status == "Not On Call":
            ACTIVE_CALLS.pop(call_id, None)
        else:
            ACTIVE_CALLS[call_id] = {
                "callId": call_id,
                "callSessionId": call_session_id,
                "userKey": user_key,
                "status": status,
                "webexState": data.get("state"),
                "eventType": data.get("eventType") or event.get("event"),
                "updatedAt": now_iso(),
            }

    existing = USERS.get(user_key, {})

    USERS[user_key] = {
        "userKey": user_key,
        "personId": existing.get("personId") or event.get("actorId") or event.get("createdBy"),
        "displayName": existing.get("displayName") or existing.get("email") or user_key,
        "email": existing.get("email"),
        "status": status,
        "webexState": data.get("state"),
        "eventType": data.get("eventType") or event.get("event"),
        "callId": None if status == "Not On Call" else call_id,
        "callSessionId": None if status == "Not On Call" else call_session_id,
        "remoteName": None if status == "Not On Call" else remote.get("name"),
        "remoteNumber": None if status == "Not On Call" else remote.get("number"),
        "remoteCallType": None if status == "Not On Call" else remote.get("callType"),
        "updatedAt": now_iso(),
        "webhookId": event.get("id") or existing.get("webhookId"),
    }

    return {"status": "received", "userStatus": USERS[user_key]}
