import os
import json
import sqlite3
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI(title="Webex Calling Supervisor Dashboard")

WEBEX_CLIENT_ID = os.getenv("WEBEX_CLIENT_ID")
WEBEX_CLIENT_SECRET = os.getenv("WEBEX_CLIENT_SECRET")
WEBEX_REDIRECT_URI = os.getenv("WEBEX_REDIRECT_URI")
WEBEX_WEBHOOK_TARGET_URL = os.getenv("WEBEX_WEBHOOK_TARGET_URL")

# Optional but recommended:
# Use a Webex admin/partner token that can read organizations.
WEBEX_ADMIN_TOKEN = os.getenv("WEBEX_ADMIN_TOKEN")

# Optional manual fallback:
# Example:
# WEBEX_ORG_NAME_MAP={"Y2lzY29...":"Bullfrog Group","Y2lzY29...":"Customer ABC"}
WEBEX_ORG_NAME_MAP_RAW = os.getenv("WEBEX_ORG_NAME_MAP", "{}")

try:
    WEBEX_ORG_NAME_MAP = json.loads(WEBEX_ORG_NAME_MAP_RAW)
except Exception:
    WEBEX_ORG_NAME_MAP = {}

SCOPES = "spark:calls_read spark:webhooks_write spark:webhooks_read spark:people_read spark-admin:organizations_read"

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "attendant_console.db"

ORG_NAME_CACHE: Dict[str, str] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                person_id TEXT PRIMARY KEY,
                email TEXT,
                display_name TEXT,
                extension TEXT,
                org_id TEXT,
                org_name TEXT,
                status TEXT NOT NULL DEFAULT 'Not On Call',
                webex_state TEXT,
                event_type TEXT,
                call_id TEXT,
                call_session_id TEXT,
                remote_name TEXT,
                remote_number TEXT,
                remote_call_type TEXT,
                state_started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                webhook_id TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id TEXT,
                org_id TEXT,
                org_name TEXT,
                event_type TEXT,
                webex_state TEXT,
                call_id TEXT,
                call_session_id TEXT,
                payload TEXT,
                created_at TEXT NOT NULL
            )
        """)

        existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(agents)").fetchall()}
        for column_name, column_type in {
            "extension": "TEXT",
            "org_id": "TEXT",
            "org_name": "TEXT",
        }.items():
            if column_name not in existing_columns:
                conn.execute(f"ALTER TABLE agents ADD COLUMN {column_name} {column_type}")

        event_columns = {row[1] for row in conn.execute("PRAGMA table_info(events)").fetchall()}
        for column_name, column_type in {
            "org_id": "TEXT",
            "org_name": "TEXT",
        }.items():
            if column_name not in event_columns:
                conn.execute(f"ALTER TABLE events ADD COLUMN {column_name} {column_type}")


init_db()


def is_unresolved_org_name(org_id: Optional[str], org_name: Optional[str]) -> bool:
    if not org_id:
        return True
    if not org_name:
        return True
    if org_name == org_id:
        return True
    if org_name.startswith("Y2lzY29"):
        return True
    return False


def resolve_org_name(org_id: Optional[str], user_access_token: Optional[str] = None) -> str:
    """
    Resolve Webex orgId to the friendly organization displayName.

    Primary lookup:
      GET https://webexapis.com/v1/organizations/{orgId}

    Expected response:
      {
        "id": "...",
        "displayName": "Customer / Org Name",
        "created": "..."
      }

    Token order:
      1. WEBEX_ADMIN_TOKEN, if configured in Render
      2. user_access_token, if passed in from the OAuth flow
      3. WEBEX_ORG_NAME_MAP manual fallback
      4. orgId fallback
    """
    if not org_id:
        return "Unknown Org"

    # Only trust the cache if it contains a real friendly name.
    # Do not permanently cache raw Webex org IDs because permissions/scopes may be fixed later.
    if org_id in ORG_NAME_CACHE:
        cached_name = ORG_NAME_CACHE[org_id]
        if cached_name and cached_name != org_id and not cached_name.startswith("Y2lzY29"):
            return cached_name
        ORG_NAME_CACHE.pop(org_id, None)

    tokens_to_try = []
    if WEBEX_ADMIN_TOKEN:
        tokens_to_try.append(WEBEX_ADMIN_TOKEN)
    if user_access_token:
        tokens_to_try.append(user_access_token)

    for token in tokens_to_try:
        try:
            response = requests.get(
                f"https://webexapis.com/v1/organizations/{org_id}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                timeout=20,
            )

            if response.status_code < 400:
                org = response.json()
                display_name = org.get("displayName")

                if display_name:
                    ORG_NAME_CACHE[org_id] = display_name
                    return display_name

                print(f"Org lookup succeeded but no displayName was returned for {org_id}: {org}")
            else:
                print(f"Org lookup failed for {org_id}: {response.status_code} {response.text}")

        except Exception as exc:
            print(f"Org lookup exception for {org_id}: {exc}")

    # Manual fallback if the API token cannot read the org.
    if org_id in WEBEX_ORG_NAME_MAP:
        name = WEBEX_ORG_NAME_MAP[org_id]
        ORG_NAME_CACHE[org_id] = name
        return name

    # Final fallback. Do not cache the raw orgId, so a later refresh can retry the lookup.
    return org_id

def refresh_known_org_names():
    """
    Re-check stored org IDs against manual map/admin token.
    This lets the UI update after you add WEBEX_ORG_NAME_MAP or WEBEX_ADMIN_TOKEN.
    """
    with db() as conn:
        rows = conn.execute("SELECT person_id, org_id, org_name FROM agents WHERE org_id IS NOT NULL").fetchall()
        for row in rows:
            org_id = row["org_id"]
            current_name = row["org_name"]
            resolved = resolve_org_name(org_id)

            if resolved and resolved != current_name and resolved != org_id:
                conn.execute(
                    "UPDATE agents SET org_name = ? WHERE person_id = ?",
                    (resolved, row["person_id"]),
                )


def classify_status(event: Dict[str, Any]) -> str:
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
        "active",
        "held",
        "remoteheld",
        "bridged",
        "consulting",
        "conference",
    } or event_type in {"answered", "connected"}:
        return "On Call"

    return "Unknown"


def extract_remote_party(event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    remote = data.get("remoteParty", {})
    return remote if isinstance(remote, dict) else {}


def extract_person_id(event: Dict[str, Any]) -> str:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    return (
        data.get("personId")
        or data.get("ownerId")
        or event.get("actorId")
        or event.get("createdBy")
        or event.get("webhookId")
        or "unknown-person"
    )


def get_me(user_access_token: str) -> Optional[Dict[str, Any]]:
    response = requests.get(
        "https://webexapis.com/v1/people/me",
        headers={"Authorization": f"Bearer {user_access_token}"},
        timeout=20,
    )

    if response.status_code >= 400:
        print("Unable to call /people/me:", response.text)
        return None

    return response.json()


def create_call_status_webhook(user_access_token: str) -> Dict[str, Any]:
    if not WEBEX_WEBHOOK_TARGET_URL:
        raise HTTPException(status_code=500, detail="Missing WEBEX_WEBHOOK_TARGET_URL")

    headers = {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json",
    }

    def find_existing_webhook() -> Optional[Dict[str, Any]]:
        list_response = requests.get(
            "https://webexapis.com/v1/webhooks",
            headers=headers,
            timeout=20,
        )

        if list_response.status_code >= 400:
            print("Unable to list existing webhooks:", list_response.text)
            return None

        existing_webhooks = list_response.json().get("items", [])

        for webhook in existing_webhooks:
            if (
                webhook.get("resource") == "telephony_calls"
                and webhook.get("targetUrl") == WEBEX_WEBHOOK_TARGET_URL
                and webhook.get("event") in {"all", "created", "updated", "deleted"}
            ):
                print(f"Reusing existing webhook: {webhook.get('id')}")
                return webhook

        return None

    existing = find_existing_webhook()
    if existing:
        return existing

    payload = {
        "name": "Supervisor Dashboard - Webex Calling Status",
        "targetUrl": WEBEX_WEBHOOK_TARGET_URL,
        "resource": "telephony_calls",
        "event": "all",
    }

    create_response = requests.post(
        "https://webexapis.com/v1/webhooks",
        json=payload,
        headers=headers,
        timeout=20,
    )

    if create_response.status_code == 409:
        print("Webhook create returned 409 duplicate. Attempting to reuse existing webhook.")
        duplicate = find_existing_webhook()
        if duplicate:
            return duplicate

        raise HTTPException(
            status_code=409,
            detail="A duplicate webhook exists, but the app could not retrieve it.",
        )

    if create_response.status_code >= 400:
        raise HTTPException(status_code=create_response.status_code, detail=create_response.text)

    return create_response.json()



def extract_extension_from_calling_person(payload: Dict[str, Any]) -> Optional[str]:
    """
    Webex Calling person payloads can vary by endpoint/version.
    This checks the common places extension may appear.
    """
    if not payload:
        return None

    for key in ("extension", "primaryExtension"):
        value = payload.get(key)
        if value:
            return str(value)

    # Sometimes numbers are represented as objects.
    for key in ("phoneNumbers", "numbers", "directNumbers"):
        items = payload.get(key)
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    ext = item.get("extension") or item.get("primaryExtension")
                    if ext:
                        return str(ext)

    # Sometimes calling details are nested.
    calling = payload.get("calling")
    if isinstance(calling, dict):
        ext = calling.get("extension") or calling.get("primaryExtension")
        if ext:
            return str(ext)

    return None


def resolve_user_extension(person_id: Optional[str], org_id: Optional[str], user_access_token: Optional[str] = None) -> Optional[str]:
    """
    Best-effort lookup for the user's Webex Calling extension.

    Primary:
      GET /v1/telephony/config/people/{personId}?orgId={orgId}

    Token order:
      1. WEBEX_ADMIN_TOKEN
      2. user OAuth token from connect flow

    This usually requires an admin/partner token with telephony config read permission.
    """
    if not person_id:
        return None

    tokens_to_try = []
    if WEBEX_ADMIN_TOKEN:
        tokens_to_try.append(WEBEX_ADMIN_TOKEN)
    if user_access_token:
        tokens_to_try.append(user_access_token)

    for token in tokens_to_try:
        try:
            url = f"https://webexapis.com/v1/telephony/config/people/{person_id}"
            params = {}
            if org_id:
                params["orgId"] = org_id

            response = requests.get(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                params=params,
                timeout=20,
            )

            if response.status_code < 400:
                extension = extract_extension_from_calling_person(response.json())
                if extension:
                    return extension
                print(f"Extension lookup returned no extension for {person_id}: {response.text}")
            else:
                print(f"Extension lookup failed for {person_id}: {response.status_code} {response.text}")

        except Exception as exc:
            print(f"Extension lookup exception for {person_id}: {exc}")

    return None



def upsert_agent_from_oauth(me: Dict[str, Any], webhook: Dict[str, Any], user_access_token: str):
    emails = me.get("emails") or []
    email = emails[0] if emails else None
    person_id = me.get("id")
    display_name = me.get("displayName") or email or person_id or "Unknown User"

    org_id = me.get("orgId")
    org_name = resolve_org_name(org_id, user_access_token)
    extension = resolve_user_extension(person_id, org_id, user_access_token)

    if not person_id:
        return

    ts = now_iso()

    with db() as conn:
        existing = conn.execute(
            "SELECT person_id FROM agents WHERE person_id = ?",
            (person_id,),
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE agents
                SET email = ?, display_name = ?, extension = COALESCE(?, extension),
                    org_id = ?, org_name = ?, webhook_id = ?, updated_at = ?
                WHERE person_id = ?
            """, (email, display_name, extension, org_id, org_name, webhook.get("id"), ts, person_id))
        else:
            conn.execute("""
                INSERT INTO agents (
                    person_id, email, display_name, extension, org_id, org_name,
                    status, state_started_at, updated_at, webhook_id
                )
                VALUES (?, ?, ?, ?, ?, ?, 'Not On Call', ?, ?, ?)
            """, (person_id, email, display_name, extension, org_id, org_name, ts, ts, webhook.get("id")))


def update_agent_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    remote = extract_remote_party(event)

    person_id = extract_person_id(event)
    org_id = event.get("orgId")
    org_name = resolve_org_name(org_id)
    extension = resolve_user_extension(person_id, org_id)

    new_status = classify_status(event)

    webex_state = data.get("state")
    event_type = data.get("eventType") or event.get("event")
    call_id = data.get("callId")
    call_session_id = data.get("callSessionId")

    if new_status == "Not On Call":
        # Once the call is over, clear call-specific values so the UI shows N/A.
        webex_state = None
        event_type = None
        call_id = None
        call_session_id = None
        remote_name = None
        remote_number = None
        remote_call_type = None
    else:
        remote_name = remote.get("name")
        remote_number = remote.get("number")
        remote_call_type = remote.get("callType")

    ts = now_iso()

    with db() as conn:
        existing = conn.execute(
            "SELECT * FROM agents WHERE person_id = ?",
            (person_id,),
        ).fetchone()

        if existing:
            state_started_at = existing["state_started_at"] if existing["status"] == new_status else ts
            webhook_id = event.get("id") or existing["webhook_id"]

            # If we could not resolve the org name, preserve a previously resolved friendly name.
            if org_name == org_id and existing["org_name"] and existing["org_name"] != existing["org_id"]:
                org_name = existing["org_name"]

            conn.execute("""
                UPDATE agents
                SET status = ?, extension = COALESCE(?, extension),
                    org_id = COALESCE(?, org_id), org_name = COALESCE(?, org_name),
                    webex_state = ?, event_type = ?, call_id = ?, call_session_id = ?,
                    remote_name = ?, remote_number = ?, remote_call_type = ?,
                    state_started_at = ?, updated_at = ?, webhook_id = ?
                WHERE person_id = ?
            """, (
                new_status,
                extension,
                org_id,
                org_name,
                webex_state,
                event_type,
                call_id,
                call_session_id,
                remote_name,
                remote_number,
                remote_call_type,
                state_started_at,
                ts,
                webhook_id,
                person_id,
            ))
        else:
            conn.execute("""
                INSERT INTO agents (
                    person_id, email, display_name, extension, org_id, org_name, status,
                    webex_state, event_type, call_id, call_session_id,
                    remote_name, remote_number, remote_call_type,
                    state_started_at, updated_at, webhook_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                person_id,
                person_id,
                person_id,
                extension,
                org_id,
                org_name,
                new_status,
                webex_state,
                event_type,
                call_id,
                call_session_id,
                remote_name,
                remote_number,
                remote_call_type,
                ts,
                ts,
                event.get("id"),
            ))

        conn.execute("""
            INSERT INTO events (
                person_id, org_id, org_name, event_type, webex_state,
                call_id, call_session_id, payload, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            person_id,
            org_id,
            org_name,
            event_type,
            webex_state,
            data.get("callId"),
            data.get("callSessionId"),
            json.dumps(event),
            ts,
        ))

        row = conn.execute(
            "SELECT * FROM agents WHERE person_id = ?",
            (person_id,),
        ).fetchone()

        return dict(row)


@app.get("/", response_class=HTMLResponse)
def root():
    return HTMLResponse("""
    <html>
      <head>
        <title>Webex Calling Status Connector</title>
        <style>
          body {
            font-family: Arial, sans-serif;
            background: #eef3f8;
            padding: 40px;
            color: #101828;
          }
          .card {
            background: white;
            padding: 24px;
            border-radius: 16px;
            max-width: 720px;
            box-shadow: 0 8px 18px rgba(15,23,42,.08);
          }
          a.button {
            display: inline-block;
            margin-top: 12px;
            background: #2563eb;
            color: white;
            text-decoration: none;
            padding: 10px 14px;
            border-radius: 10px;
          }
          .muted {
            color: #667085;
            font-size: 14px;
          }
        </style>
      </head>
      <body>
        <div class="card">
          <h2>Webex Calling Status Connector</h2>
          <p>This page is used to connect a Webex Calling user to the status monitor.</p>
          <p class="muted">After connecting, you will see a confirmation message only.</p>
          <a class="button" href="/oauth/start">Connect Webex User</a>
          <p class="muted">To disconnect later, use <strong>/oauth/remove/start</strong>.</p>
        </div>
      </body>
    </html>
    """)


@app.get("/health")
def health():
    return {"status": "ok", "database": str(DB_PATH)}


@app.get("/supervisor", response_class=HTMLResponse)
def supervisor_dashboard():
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Webex Calling Supervisor Dashboard</title>
  <style>
    :root {
      --bg: #eef3f8;
      --panel: #ffffff;
      --text: #101828;
      --muted: #667085;
      --border: #d0d5dd;
      --blue: #2563eb;
      --green-bg: #dcfce7;
      --green-text: #166534;
      --red-bg: #fee2e2;
      --red-text: #991b1b;
      --yellow-bg: #fef3c7;
      --yellow-text: #92400e;
      --gray-bg: #e5e7eb;
      --gray-text: #374151;
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
      padding: 24px 30px;
    }

    header h1 { margin: 0; font-size: 26px; }
    header p { margin: 7px 0 0; color: #cbd5e1; font-size: 14px; }

    main {
      max-width: 1500px;
      margin: 0 auto;
      padding: 22px;
    }

    .toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 16px;
    }

    .toolbar-left {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }

    input, select {
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 14px;
      min-width: 240px;
      background: white;
    }

    button, a.button {
      border: none;
      border-radius: 10px;
      background: var(--blue);
      color: white;
      padding: 10px 14px;
      font-size: 14px;
      cursor: pointer;
      text-decoration: none;
      display: inline-block;
    }

    button.secondary { background: #475569; }

    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(150px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }

    .summary-card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06);
    }

    .summary-card .label { color: var(--muted); font-size: 13px; }
    .summary-card .value { font-size: 30px; font-weight: 800; margin-top: 4px; }

    .table-wrap {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }

    thead { background: #f8fafc; }

    th {
      text-align: left;
      padding: 13px 14px;
      color: #475569;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      border-bottom: 1px solid var(--border);
      white-space: nowrap;
    }

    td {
      padding: 13px 14px;
      border-bottom: 1px solid #eef2f7;
      vertical-align: middle;
    }

    tr:last-child td { border-bottom: none; }
    tbody tr:hover { background: #f8fafc; }

    .email { font-weight: 800; color: #0f172a; word-break: break-word; }
    .subtext { color: var(--muted); font-size: 12px; margin-top: 2px; word-break: break-word; }

    .pill {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 7px 11px;
      font-weight: 800;
      font-size: 12px;
      white-space: nowrap;
    }

    .Ringing { background: var(--yellow-bg); color: var(--yellow-text); }
    .OnCall { background: var(--red-bg); color: var(--red-text); }
    .NotOnCall { background: var(--green-bg); color: var(--green-text); }
    .Unknown { background: var(--gray-bg); color: var(--gray-text); }

    .duration { font-weight: 800; color: #0f172a; white-space: nowrap; }
    .empty { padding: 36px; text-align: center; color: var(--muted); }

    @media (max-width: 900px) {
      main { padding: 14px; }
      .summary { grid-template-columns: repeat(2, 1fr); }
      .table-wrap { overflow-x: auto; }
      table { min-width: 1180px; }
      input, select, button, a.button { width: 100%; }
      .toolbar, .toolbar-left { width: 100%; align-items: stretch; }
    }
  </style>
</head>
<body>
<header>
  <h1>Webex Calling Supervisor Dashboard</h1>
  <p>Monitor users by organization, current state, and duration.</p>
</header>

<main>
  <div class="toolbar">
    <div class="toolbar-left">
      <input id="search" placeholder="Search email, org, name, number..." oninput="renderTable()" />
      <select id="orgFilter" onchange="renderTable()">
        <option value="All">All Organizations</option>
      </select>
      <select id="stateFilter" onchange="renderTable()">
        <option value="All">All States</option>
        <option value="Ringing">Ringing</option>
        <option value="On Call">On Call</option>
        <option value="Not On Call">Not On Call</option>
        <option value="Unknown">Unknown</option>
      </select>
      <button onclick="loadAgents()">Refresh</button>
      <button class="secondary" onclick="resetAgents()">Reset</button>
    </div>
    <a class="button" href="/oauth/start">Connect Webex User</a>
  </div>

  <section class="summary">
    <div class="summary-card"><div class="label">Total Users</div><div class="value" id="totalCount">0</div></div>
    <div class="summary-card"><div class="label">Ringing</div><div class="value" id="ringingCount">0</div></div>
    <div class="summary-card"><div class="label">On Call</div><div class="value" id="onCallCount">0</div></div>
    <div class="summary-card"><div class="label">Not On Call</div><div class="value" id="notOnCallCount">0</div></div>
  </section>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>User Email</th>
          <th>Organization</th>
          <th>State</th>
          <th>Time in State</th>
          <th>Display Name</th>
          <th>Webex State</th>
          <th>Event Type</th>
          <th>Remote Party</th>
          <th>Remote Number</th>
          <th>Extension</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody id="agentBody">
        <tr><td colspan="11" class="empty">Loading...</td></tr>
      </tbody>
    </table>
  </div>


</main>

<script>
let agents = [];

function cssStatus(status) {
  if (status === "On Call") return "OnCall";
  if (status === "Not On Call") return "NotOnCall";
  if (status === "Ringing") return "Ringing";
  return "Unknown";
}

function fmtDate(value) {
  if (!value) return "N/A";
  try { return new Date(value).toLocaleString(); } catch { return value; }
}

function durationSince(value) {
  if (!value) return "N/A";
  const start = new Date(value).getTime();
  if (Number.isNaN(start)) return "N/A";
  const secondsTotal = Math.max(0, Math.floor((Date.now() - start) / 1000));
  const h = Math.floor(secondsTotal / 3600);
  const m = Math.floor((secondsTotal % 3600) / 60);
  const s = secondsTotal % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function populateOrgFilter() {
  const select = document.getElementById("orgFilter");
  const selected = select.value;
  const orgs = [...new Set(agents.map(a => a.org_name || a.org_id || "Unknown Org"))].sort();

  select.innerHTML = `<option value="All">All Organizations</option>` +
    orgs.map(org => `<option value="${org}">${org}</option>`).join("");

  if ([...select.options].some(o => o.value === selected)) {
    select.value = selected;
  }
}

async function loadAgents() {
  try {
    const res = await fetch("/api/agents", { cache: "no-store" });
    const data = await res.json();
    agents = data.agents || [];
    populateOrgFilter();
    renderTable();
  } catch (err) {
    console.error(err);
    document.getElementById("agentBody").innerHTML =
      `<tr><td colspan="11" class="empty">Could not load /api/agents. Check Render logs.</td></tr>`;
  }
}

function renderSummary() {
  document.getElementById("totalCount").textContent = agents.length;
  document.getElementById("ringingCount").textContent = agents.filter(a => a.status === "Ringing").length;
  document.getElementById("onCallCount").textContent = agents.filter(a => a.status === "On Call").length;
  document.getElementById("notOnCallCount").textContent = agents.filter(a => a.status === "Not On Call").length;
}

function renderTable() {
  renderSummary();

  const search = document.getElementById("search").value.toLowerCase().trim();
  const orgFilter = document.getElementById("orgFilter").value;
  const stateFilter = document.getElementById("stateFilter").value;

  const filtered = agents.filter(a => {
    const orgName = a.org_name || a.org_id || "Unknown Org";
    const matchesOrg = orgFilter === "All" || orgName === orgFilter;
    const matchesState = stateFilter === "All" || a.status === stateFilter;
    const blob = JSON.stringify(a).toLowerCase();
    return matchesOrg && matchesState && blob.includes(search);
  });

  const body = document.getElementById("agentBody");

  if (!filtered.length) {
    body.innerHTML = `<tr><td colspan="11" class="empty">No users match the current filters.</td></tr>`;
    return;
  }

  body.innerHTML = filtered.map(a => {
    const orgName = a.org_name || a.org_id || "Unknown Org";
    return `
      <tr>
        <td>
          <div class="email">${a.email || "Unknown"}</div>
        </td>
        <td>
          <div class="email">${orgName}</div>
        </td>
        <td><span class="pill ${cssStatus(a.status)}">${a.status || "Unknown"}</span></td>
        <td><span class="duration">${durationSince(a.state_started_at || a.updated_at)}</span></td>
        <td>${a.display_name || "N/A"}</td>
        <td>${a.webex_state || "N/A"}</td>
        <td>${a.event_type || "N/A"}</td>
        <td>${a.remote_name || "N/A"}</td>
        <td>${a.remote_number || "N/A"}</td>
        <td>${a.extension || "N/A"}</td>
        <td>
          <button class="secondary" onclick="removeAgent('${a.person_id}')">Remove Row</button>
        </td>
      </tr>
    `;
  }).join("");
}

async function removeAgent(personId) {
  if (!confirm("Remove this user from the dashboard? This only removes the local row. To delete the Webex webhook, the user should use /oauth/remove/start.")) {
    return;
  }

  await fetch(`/api/agents/${encodeURIComponent(personId)}/remove`, { method: "POST" });
  await loadAgents();
}

async function resetAgents() {
  await fetch("/api/reset", { method: "POST" });
  await loadAgents();
}

loadAgents();
setInterval(loadAgents, 3000);
setInterval(renderTable, 1000);
</script>
</body>
</html>
    """)



@app.post("/api/refresh-orgs")
def api_refresh_orgs():
    ORG_NAME_CACHE.clear()
    refresh_known_org_names()
    return {"message": "organization name cache cleared and refreshed"}


@app.post("/api/refresh-extensions")
def api_refresh_extensions():
    """
    Refresh extensions for all known users using WEBEX_ADMIN_TOKEN.
    """
    updated = 0

    with db() as conn:
        rows = conn.execute("SELECT person_id, org_id FROM agents").fetchall()

        for row in rows:
            extension = resolve_user_extension(row["person_id"], row["org_id"])
            if extension:
                conn.execute(
                    "UPDATE agents SET extension = ?, updated_at = ? WHERE person_id = ?",
                    (extension, now_iso(), row["person_id"]),
                )
                updated += 1

    return {"message": "extension refresh complete", "updated": updated}


@app.get("/api/agents")
def api_agents():
    # Try to refresh org names on each API call, so adding WEBEX_ORG_NAME_MAP or WEBEX_ADMIN_TOKEN
    # updates the UI without reconnecting every user.
    refresh_known_org_names()

    with db() as conn:
        rows = conn.execute("SELECT * FROM agents").fetchall()

    agents = [dict(row) for row in rows]

    for agent in agents:
        org_id = agent.get("org_id")
        org_name = agent.get("org_name")

        if is_unresolved_org_name(org_id, org_name):
            agent["org_name"] = resolve_org_name(org_id)
        else:
            agent["org_name"] = org_name

    priority = {"Ringing": 0, "On Call": 1, "Unknown": 2, "Not On Call": 3}
    agents.sort(key=lambda a: (
        a.get("org_name") or "",
        priority.get(a.get("status"), 9),
        a.get("email") or a.get("display_name") or "",
    ))

    return {"count": len(agents), "agents": agents}


@app.get("/api/events")
def api_events(limit: int = 50):
    with db() as conn:
        rows = conn.execute("""
            SELECT id, person_id, org_id, org_name, event_type, webex_state,
                   call_id, call_session_id, created_at
            FROM events
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()

    return {"events": [dict(row) for row in rows]}



@app.post("/api/agents/{person_id}/remove")
def api_remove_agent(person_id: str):
    """
    Removes the user from the local supervisor dashboard only.
    If their Webex webhook still exists, they may reappear when a new call event arrives.
    To delete the actual webhook, have the user visit /oauth/remove/start.
    """
    remove_agent_from_dashboard(person_id)
    return {
        "message": "agent removed from dashboard",
        "note": "Webhook is not deleted by this endpoint. Use /oauth/remove/start to disconnect the Webex webhook."
    }


@app.post("/api/reset")
def api_reset():
    with db() as conn:
        conn.execute("DELETE FROM agents")
        conn.execute("DELETE FROM events")

    return {"message": "reset complete"}



@app.get("/oauth/remove/start")
def oauth_remove_start():
    """
    User-facing disconnect flow. The user signs in with Webex, then the app deletes
    their telephony_calls webhook for this target URL and removes them from the dashboard.
    """
    if not WEBEX_CLIENT_ID or not WEBEX_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing WEBEX_CLIENT_ID or WEBEX_REDIRECT_URI")

    remove_redirect_uri = WEBEX_REDIRECT_URI.replace("/oauth/callback", "/oauth/remove/callback")

    auth_url = (
        "https://webexapis.com/v1/authorize"
        f"?client_id={WEBEX_CLIENT_ID}"
        "&response_type=code"
        f"&redirect_uri={remove_redirect_uri}"
        f"&scope={SCOPES.replace(' ', '%20')}"
        "&state=webex-calling-disconnect"
    )

    return RedirectResponse(auth_url)


@app.get("/oauth/remove/callback")
def oauth_remove_callback(request: Request):
    code = request.query_params.get("code")

    if not code:
        error = request.query_params.get("error")
        error_description = request.query_params.get("error_description")
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Missing authorization code from Webex.",
                "error": error,
                "error_description": error_description,
                "fix": "Start disconnect OAuth from /oauth/remove/start."
            }
        )

    if not WEBEX_CLIENT_ID or not WEBEX_CLIENT_SECRET or not WEBEX_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing Webex OAuth environment variables")

    remove_redirect_uri = WEBEX_REDIRECT_URI.replace("/oauth/callback", "/oauth/remove/callback")

    token_response = requests.post(
        "https://webexapis.com/v1/access_token",
        data={
            "grant_type": "authorization_code",
            "client_id": WEBEX_CLIENT_ID,
            "client_secret": WEBEX_CLIENT_SECRET,
            "code": code,
            "redirect_uri": remove_redirect_uri,
        },
        timeout=20,
    )

    if token_response.status_code >= 400:
        raise HTTPException(status_code=token_response.status_code, detail=token_response.text)

    tokens = token_response.json()
    access_token = tokens.get("access_token")

    me = get_me(access_token)
    delete_result = delete_call_status_webhooks_for_user(access_token)

    if me and me.get("id"):
        remove_agent_from_dashboard(me.get("id"))

    return HTMLResponse(f"""
    <html>
      <head>
        <title>Webex User Disconnected</title>
        <style>
          body {{ font-family: Arial, sans-serif; background: #eef3f8; padding: 40px; color: #101828; }}
          .card {{ background: white; padding: 24px; border-radius: 16px; max-width: 720px; box-shadow: 0 8px 18px rgba(15,23,42,.08); }}
          .success {{ color: #166534; font-weight: 800; }}
          .muted {{ color: #667085; font-size: 14px; }}
        </style>
      </head>
      <body>
        <div class="card">
          <h2 class="success">Disconnected Successfully</h2>
          <p>Your Webex Calling status connection has been removed.</p>
          <p class="muted">Deleted webhook count: {len(delete_result.get("deleted", []))}</p>
          <p class="muted">You can close this browser tab now.</p>
        </div>
      </body>
    </html>
    """)


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
        "&state=webex-calling-supervisor-dashboard"
    )

    return RedirectResponse(auth_url)


@app.get("/oauth/callback")
def oauth_callback(request: Request):
    code = request.query_params.get("code")

    if not code:
        raise HTTPException(status_code=400, detail="Missing authorization code")

    if not WEBEX_CLIENT_ID or not WEBEX_CLIENT_SECRET or not WEBEX_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing Webex OAuth environment variables")

    token_response = requests.post(
        "https://webexapis.com/v1/access_token",
        data={
            "grant_type": "authorization_code",
            "client_id": WEBEX_CLIENT_ID,
            "client_secret": WEBEX_CLIENT_SECRET,
            "code": code,
            "redirect_uri": WEBEX_REDIRECT_URI,
        },
        timeout=20,
    )

    if token_response.status_code >= 400:
        raise HTTPException(status_code=token_response.status_code, detail=token_response.text)

    tokens = token_response.json()
    access_token = tokens.get("access_token")

    me = get_me(access_token)
    webhook = create_call_status_webhook(access_token)

    if me:
        upsert_agent_from_oauth(me, webhook, access_token)

    return HTMLResponse(f"""
    <html>
      <head>
        <title>Webex User Connected</title>
        <style>
          body {{ font-family: Arial, sans-serif; background: #eef3f8; padding: 40px; color: #101828; }}
          .card {{ background: white; padding: 24px; border-radius: 16px; max-width: 720px; box-shadow: 0 8px 18px rgba(15,23,42,.08); }}
          .success {{ color: #166534; font-weight: 800; }}
          .muted {{ color: #667085; font-size: 14px; }}
        </style>
      </head>
      <body>
        <div class="card">
          <h2 class="success">Connection Successful</h2>
          <p>Your Webex Calling status connection has been completed.</p>
          <p class="muted">You can close this browser tab now.</p>
        </div>
      </body>
    </html>
    """)


@app.post("/webex/calling-events")
async def calling_events(request: Request):
    event = await request.json()

    print("Received Webex Calling event:")
    print(json.dumps(event, indent=2))

    agent = update_agent_from_event(event)

    return {"status": "received", "agent": agent}
