import os
import json
import sqlite3
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI(title="Webex Calling Supervisor Dashboard")

WEBEX_CLIENT_ID = os.getenv("WEBEX_CLIENT_ID")
WEBEX_CLIENT_SECRET = os.getenv("WEBEX_CLIENT_SECRET")
WEBEX_REDIRECT_URI = os.getenv("WEBEX_REDIRECT_URI")
WEBEX_WEBHOOK_TARGET_URL = os.getenv("WEBEX_WEBHOOK_TARGET_URL")
WEBEX_ADMIN_TOKEN = os.getenv("WEBEX_ADMIN_TOKEN")

# Optional manual fallback:
# WEBEX_ORG_NAME_MAP={"orgIdHere":"Friendly Org Name"}
WEBEX_ORG_NAME_MAP_RAW = os.getenv("WEBEX_ORG_NAME_MAP", "{}")
try:
    WEBEX_ORG_NAME_MAP = json.loads(WEBEX_ORG_NAME_MAP_RAW)
except Exception:
    WEBEX_ORG_NAME_MAP = {}

# Keep user OAuth scopes focused on what the user needs to authorize.
# Use WEBEX_ADMIN_TOKEN for org displayName and extension enrichment.
SCOPES = "spark:calls_read spark:webhooks_write spark:webhooks_read spark:people_read spark-admin:organizations_read spark-admin:people_read"

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

        agent_columns = {row["name"] for row in conn.execute("PRAGMA table_info(agents)").fetchall()}
        for column_name in ["extension", "org_id", "org_name"]:
            if column_name not in agent_columns:
                conn.execute(f"ALTER TABLE agents ADD COLUMN {column_name} TEXT")

        event_columns = {row["name"] for row in conn.execute("PRAGMA table_info(events)").fetchall()}
        for column_name in ["org_id", "org_name"]:
            if column_name not in event_columns:
                conn.execute(f"ALTER TABLE events ADD COLUMN {column_name} TEXT")


init_db()


def build_webex_authorize_url(redirect_uri: str, state: str) -> str:
    params = {
        "client_id": WEBEX_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": SCOPES,
        "state": state,
    }
    return "https://webexapis.com/v1/authorize?" + urlencode(params)


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


def resolve_org_name(org_id: Optional[str], user_access_token: Optional[str] = None) -> str:
    """
    Resolve the long Webex orgId to the friendly organization displayName.

    It tries:
      1. WEBEX_ORG_NAME_MAP manual override
      2. In-memory cache, but only if the cached value is a friendly name
      3. WEBEX_ADMIN_TOKEN, if set
      4. The signing-in user's OAuth token, if provided
      5. Raw orgId fallback
    """
    if not org_id:
        return "Unknown Org"

    if org_id in WEBEX_ORG_NAME_MAP:
        ORG_NAME_CACHE[org_id] = WEBEX_ORG_NAME_MAP[org_id]
        return WEBEX_ORG_NAME_MAP[org_id]

    cached = ORG_NAME_CACHE.get(org_id)
    if cached and cached != org_id and not cached.startswith("Y2lzY29"):
        return cached

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
                data = response.json()
                display_name = data.get("displayName")
                if display_name:
                    ORG_NAME_CACHE[org_id] = display_name
                    return display_name

                print(f"Org lookup succeeded but no displayName returned for {org_id}: {data}")
            else:
                print(f"Org lookup failed for {org_id}: {response.status_code} {response.text}")

        except Exception as exc:
            print(f"Org lookup exception for {org_id}: {exc}")

    # Do not cache the raw orgId as a failed value. That way if scopes/tokens are fixed later,
    # the app can retry and replace it with displayName.
    return org_id

def extract_work_extension(person_payload: Dict[str, Any]) -> Optional[str]:
    phone_numbers = person_payload.get("phoneNumbers", [])
    if isinstance(phone_numbers, list):
        for phone in phone_numbers:
            if isinstance(phone, dict):
                if phone.get("type") == "work_extension" and phone.get("value"):
                    return str(phone.get("value"))
    return None


def resolve_user_extension(person_id: Optional[str], user_access_token: Optional[str] = None) -> Optional[str]:
    if not person_id:
        return None

    tokens_to_try = []
    if WEBEX_ADMIN_TOKEN:
        tokens_to_try.append(WEBEX_ADMIN_TOKEN)
    if user_access_token:
        tokens_to_try.append(user_access_token)

    for token in tokens_to_try:
        try:
            response = requests.get(
                f"https://webexapis.com/v1/people/{person_id}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                timeout=20,
            )

            if response.status_code < 400:
                extension = extract_work_extension(response.json())
                if extension:
                    return extension

                print(f"No work_extension found for {person_id}: {response.text}")
            else:
                print(f"People lookup failed for {person_id}: {response.status_code} {response.text}")
        except Exception as exc:
            print(f"People lookup exception for {person_id}: {exc}")

    return None


def classify_status(event: Dict[str, Any]) -> str:
    webhook_event = str(event.get("event", "")).lower()
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    state = str(data.get("state", "")).lower()
    event_type = str(data.get("eventType", "")).lower()

    if webhook_event == "deleted" or event_type in {"ended", "released", "disconnected"}:
        return "Not On Call"

    if state in {"alerting", "ringing"} or event_type in {"received", "offered"}:
        return "Ringing"

    if state in {"connected", "active", "held", "remoteheld", "bridged", "consulting", "conference"} or event_type in {"answered", "connected"}:
        return "On Call"

    return "Unknown"


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


def extract_remote_party(event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    remote = data.get("remoteParty", {})
    return remote if isinstance(remote, dict) else {}


def create_call_status_webhook(user_access_token: str) -> Dict[str, Any]:
    if not WEBEX_WEBHOOK_TARGET_URL:
        raise HTTPException(status_code=500, detail="Missing WEBEX_WEBHOOK_TARGET_URL")

    headers = {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json",
    }

    def find_existing_webhook() -> Optional[Dict[str, Any]]:
        response = requests.get(
            "https://webexapis.com/v1/webhooks",
            headers=headers,
            timeout=20,
        )
        if response.status_code >= 400:
            print("Unable to list webhooks:", response.text)
            return None

        for webhook in response.json().get("items", []):
            if (
                webhook.get("resource") == "telephony_calls"
                and webhook.get("targetUrl") == WEBEX_WEBHOOK_TARGET_URL
                and webhook.get("event") in {"all", "created", "updated", "deleted"}
            ):
                return webhook

        return None

    existing = find_existing_webhook()
    if existing:
        print(f"Reusing existing webhook: {existing.get('id')}")
        return existing

    payload = {
        "name": "Supervisor Dashboard - Webex Calling Status",
        "targetUrl": WEBEX_WEBHOOK_TARGET_URL,
        "resource": "telephony_calls",
        "event": "all",
    }

    response = requests.post(
        "https://webexapis.com/v1/webhooks",
        json=payload,
        headers=headers,
        timeout=20,
    )

    if response.status_code == 409:
        duplicate = find_existing_webhook()
        if duplicate:
            return duplicate

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


def delete_call_status_webhooks_for_user(user_access_token: str) -> Dict[str, Any]:
    if not WEBEX_WEBHOOK_TARGET_URL:
        raise HTTPException(status_code=500, detail="Missing WEBEX_WEBHOOK_TARGET_URL")

    headers = {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json",
    }

    response = requests.get(
        "https://webexapis.com/v1/webhooks",
        headers=headers,
        timeout=20,
    )

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    deleted = []
    skipped = []

    for webhook in response.json().get("items", []):
        if webhook.get("resource") == "telephony_calls" and webhook.get("targetUrl") == WEBEX_WEBHOOK_TARGET_URL:
            webhook_id = webhook.get("id")
            delete_response = requests.delete(
                f"https://webexapis.com/v1/webhooks/{webhook_id}",
                headers=headers,
                timeout=20,
            )
            if delete_response.status_code in (200, 202, 204):
                deleted.append(webhook_id)
            else:
                skipped.append({
                    "webhookId": webhook_id,
                    "statusCode": delete_response.status_code,
                    "error": delete_response.text,
                })

    return {"deleted": deleted, "skipped": skipped}


def upsert_agent_from_oauth(me: Dict[str, Any], webhook: Dict[str, Any], user_access_token: str):
    emails = me.get("emails") or []
    email = emails[0] if emails else None
    person_id = me.get("id")
    display_name = me.get("displayName") or email or person_id or "Unknown User"
    org_id = me.get("orgId")
    org_name = resolve_org_name(org_id, user_access_token)
    extension = resolve_user_extension(person_id, user_access_token)

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


def remove_agent_from_dashboard(person_id: str):
    with db() as conn:
        conn.execute("DELETE FROM agents WHERE person_id = ?", (person_id,))
        conn.execute("DELETE FROM events WHERE person_id = ?", (person_id,))


def update_agent_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    remote = extract_remote_party(event)

    person_id = extract_person_id(event)
    org_id = event.get("orgId")
    org_name = resolve_org_name(org_id)
    extension = resolve_user_extension(person_id)

    new_status = classify_status(event)
    webex_state = data.get("state")
    event_type = data.get("eventType") or event.get("event")
    call_id = data.get("callId")
    call_session_id = data.get("callSessionId")

    if new_status == "Not On Call":
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
                new_status, extension, org_id, org_name,
                webex_state, event_type, call_id, call_session_id,
                remote_name, remote_number, remote_call_type,
                state_started_at, ts, event.get("id") or existing["webhook_id"],
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
                person_id, person_id, person_id, extension, org_id, org_name, new_status,
                webex_state, event_type, call_id, call_session_id,
                remote_name, remote_number, remote_call_type,
                ts, ts, event.get("id"),
            ))

        conn.execute("""
            INSERT INTO events (
                person_id, org_id, org_name, event_type, webex_state,
                call_id, call_session_id, payload, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            person_id, org_id, org_name, event_type, webex_state,
            data.get("callId"), data.get("callSessionId"), json.dumps(event), ts,
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
          body { font-family: Arial, sans-serif; background: #eef3f8; padding: 40px; color: #101828; }
          .card { background: white; padding: 24px; border-radius: 16px; max-width: 720px; box-shadow: 0 8px 18px rgba(15,23,42,.08); }
          a.button { display: inline-block; margin-top: 12px; background: #2563eb; color: white; text-decoration: none; padding: 10px 14px; border-radius: 10px; }
          .muted { color: #667085; font-size: 14px; }
        </style>
      </head>
      <body>
        <div class="card">
          <h2>Webex Calling Status Connector</h2>
          <p>This page connects a Webex Calling user to the status monitor.</p>
          <a class="button" href="/oauth/start">Connect Webex User</a>
          <p class="muted">Disconnect link: /oauth/remove/start</p>
        </div>
      </body>
    </html>
    """)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "database": str(DB_PATH),
        "has_client_id": bool(WEBEX_CLIENT_ID),
        "has_redirect_uri": bool(WEBEX_REDIRECT_URI),
        "has_webhook_target": bool(WEBEX_WEBHOOK_TARGET_URL),
        "has_admin_token": bool(WEBEX_ADMIN_TOKEN),
    }


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
      --bg: #eef3f8; --panel: #ffffff; --text: #101828; --muted: #667085;
      --border: #d0d5dd; --blue: #2563eb; --green-bg: #dcfce7; --green-text: #166534;
      --red-bg: #fee2e2; --red-text: #991b1b; --yellow-bg: #fef3c7; --yellow-text: #92400e;
      --gray-bg: #e5e7eb; --gray-text: #374151;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Arial, Helvetica, sans-serif; background: var(--bg); color: var(--text); }
    header { background: linear-gradient(135deg, #0f172a, #1e293b); color: white; padding: 24px 30px; }
    header h1 { margin: 0; font-size: 26px; }
    header p { margin: 7px 0 0; color: #cbd5e1; font-size: 14px; }
    main { max-width: 1500px; margin: 0 auto; padding: 22px; }
    .toolbar { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
    .toolbar-left { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    input, select { border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; font-size: 14px; min-width: 240px; background: white; }
    button, a.button { border: none; border-radius: 10px; background: var(--blue); color: white; padding: 10px 14px; font-size: 14px; cursor: pointer; text-decoration: none; display: inline-block; }
    button.secondary { background: #475569; }
    button.small { padding: 7px 10px; font-size: 12px; }
    .summary { display: grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap: 12px; margin-bottom: 16px; }
    .summary-card { background: var(--panel); border: 1px solid var(--border); border-radius: 16px; padding: 16px; box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06); }
    .summary-card .label { color: var(--muted); font-size: 13px; }
    .summary-card .value { font-size: 30px; font-weight: 800; margin-top: 4px; }
    .table-wrap { background: var(--panel); border: 1px solid var(--border); border-radius: 16px; overflow: hidden; box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06); }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    thead { background: #f8fafc; }
    th { text-align: left; padding: 13px 14px; color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; border-bottom: 1px solid var(--border); white-space: nowrap; user-select: none; }
    th[draggable="true"] { cursor: grab; }
    th.dragging { opacity: .45; }
    th.drag-over { outline: 2px dashed var(--blue); outline-offset: -4px; }
    td { padding: 13px 14px; border-bottom: 1px solid #eef2f7; vertical-align: middle; }
    tr:last-child td { border-bottom: none; }
    tbody tr:hover { background: #f8fafc; }
    .email { font-weight: 800; color: #0f172a; word-break: break-word; }
    .pill { display: inline-flex; align-items: center; border-radius: 999px; padding: 7px 11px; font-weight: 800; font-size: 12px; white-space: nowrap; }
    .Ringing { background: var(--yellow-bg); color: var(--yellow-text); }
    .OnCall { background: var(--red-bg); color: var(--red-text); }
    .NotOnCall { background: var(--green-bg); color: var(--green-text); }
    .Unknown { background: var(--gray-bg); color: var(--gray-text); }
    .duration { font-weight: 800; color: #0f172a; white-space: nowrap; }
    .empty { padding: 36px; text-align: center; color: var(--muted); }

    .action-btn {
      border: none;
      border-radius: 10px;
      padding: 8px 11px;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
      margin-right: 6px;
      margin-bottom: 4px;
    }
    .action-btn.whisper { background: #e0f2fe; color: #075985; }
    .action-btn.barge { background: #fee2e2; color: #991b1b; }
    .action-btn:disabled {
      background: #e5e7eb;
      color: #9ca3af;
      cursor: not-allowed;
    }
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(15, 23, 42, 0.55);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 9999;
      padding: 20px;
    }
    .modal {
      background: white;
      border-radius: 18px;
      padding: 24px;
      max-width: 520px;
      width: 100%;
      box-shadow: 0 20px 45px rgba(15, 23, 42, 0.25);
    }
    .modal h2 { margin: 0 0 8px; }
    .modal p { color: #475569; line-height: 1.5; }
    .dial-code {
      background: #f1f5f9;
      border: 1px solid #cbd5e1;
      border-radius: 12px;
      padding: 14px;
      font-size: 26px;
      font-weight: 900;
      letter-spacing: .03em;
      margin: 14px 0;
      text-align: center;
    }
    .modal-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 14px;
    }
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
  <p>Monitor users by organization, current state, duration, and extension.</p>
</header>

<main>
  <div class="toolbar">
    <div class="toolbar-left">
      <input id="search" placeholder="Search email, org, extension, name, number..." oninput="renderTable()" />
      <select id="orgFilter" onchange="renderTable()"><option value="All">All Organizations</option></select>
      <select id="stateFilter" onchange="renderTable()">
        <option value="All">All States</option>
        <option value="Ringing">Ringing</option>
        <option value="On Call">On Call</option>
        <option value="Not On Call">Not On Call</option>
        <option value="Unknown">Unknown</option>
      </select>
      <button onclick="loadAgents()">Refresh</button>
      <button class="secondary" onclick="refreshExtensions()">Refresh Extensions</button>
      <button class="secondary" onclick="refreshOrgs()">Refresh Orgs</button>
      <button class="secondary" onclick="resetColumnOrder()">Reset Columns</button>
    </div>
  </div>

  <section class="summary">
    <div class="summary-card"><div class="label">Total Users</div><div class="value" id="totalCount">0</div></div>
    <div class="summary-card"><div class="label">Ringing</div><div class="value" id="ringingCount">0</div></div>
    <div class="summary-card"><div class="label">On Call</div><div class="value" id="onCallCount">0</div></div>
    <div class="summary-card"><div class="label">Not On Call</div><div class="value" id="notOnCallCount">0</div></div>
  </section>

  <div class="table-wrap">
    <table>
      <thead><tr id="tableHeader"></tr></thead>
      <tbody id="agentBody"><tr><td class="empty">Loading...</td></tr></tbody>
    </table>
  </div>

  <div id="actionModal" class="modal-backdrop">
    <div class="modal">
      <h2 id="modalTitle">Call Action</h2>
      <p id="modalText"></p>
      <div id="modalDialCode" class="dial-code"></div>
      <div class="modal-actions">
        <a id="modalDialLink" class="button" href="#">Open Dialer</a>
        <button onclick="copyDialCode()">Copy Code</button>
        <button class="secondary" onclick="closeActionModal()">Close</button>
      </div>
    </div>
  </div>

</main>

<script>
let agents = [];

const DEFAULT_COLUMNS = [
  { key: "email", label: "User Email", render: a => `<div class="email">${a.email || "Unknown"}</div>` },
  { key: "organization", label: "Organization", render: a => `<div class="email">${a.org_name || a.org_id || "Unknown Org"}</div>` },
  { key: "extension", label: "Extension", render: a => `${a.extension || "N/A"}` },
  { key: "status", label: "State", render: a => `<span class="pill ${cssStatus(a.status)}">${a.status || "Unknown"}</span>` },
  { key: "duration", label: "Time in State", render: a => `<span class="duration">${durationSince(a.state_started_at || a.updated_at)}</span>` },
  { key: "display_name", label: "Display Name", render: a => `${a.display_name || "N/A"}` },
  { key: "webex_state", label: "Webex State", render: a => `${a.webex_state || "N/A"}` },
  { key: "event_type", label: "Event Type", render: a => `${a.event_type || "N/A"}` },
  { key: "remote_name", label: "Remote Party", render: a => `${a.remote_name || "N/A"}` },
  { key: "remote_number", label: "Remote Number", render: a => `${a.remote_number || "N/A"}` },
  { key: "whisper", label: "Whisper", render: a => renderCallActionButton(a, "whisper") },
  { key: "barge", label: "Barge", render: a => renderCallActionButton(a, "barge") }
];

const STORAGE_KEY = "webexSupervisorColumnOrder";

function getColumnOrder() {
  const saved = localStorage.getItem(STORAGE_KEY);
  if (!saved) return DEFAULT_COLUMNS.map(c => c.key);
  try {
    const parsed = JSON.parse(saved);
    const validKeys = new Set(DEFAULT_COLUMNS.map(c => c.key));
    const cleaned = parsed.filter(k => validKeys.has(k));
    const missing = DEFAULT_COLUMNS.map(c => c.key).filter(k => !cleaned.includes(k));
    return [...cleaned, ...missing];
  } catch {
    return DEFAULT_COLUMNS.map(c => c.key);
  }
}

function setColumnOrder(order) { localStorage.setItem(STORAGE_KEY, JSON.stringify(order)); }
function resetColumnOrder() { localStorage.removeItem(STORAGE_KEY); renderTable(); }
function getOrderedColumns() {
  const map = Object.fromEntries(DEFAULT_COLUMNS.map(c => [c.key, c]));
  return getColumnOrder().map(k => map[k]).filter(Boolean);
}


let currentDialCode = "";

function renderCallActionButton(agent, action) {
  const isOnCall = agent.status === "On Call";
  const hasExtension = Boolean(agent.extension);
  const disabled = !(isOnCall && hasExtension);

  const label = action === "whisper" ? "Whisper" : "Barge";
  const css = action === "whisper" ? "whisper" : "barge";

  const title = !isOnCall
    ? `${label} is only available while the user is On Call`
    : (!hasExtension ? `${label} requires the user's extension` : `${label} this call`);

  return `<button class="action-btn ${css}" ${disabled ? "disabled" : ""} title="${title}" onclick="openCallAction('${action}', '${agent.extension || ""}', '${agent.email || agent.display_name || "Unknown User"}')">${label}</button>`;
}

function openCallAction(action, extension, userLabel) {
  if (!extension) return;

  const isWhisper = action === "whisper";
  const actionLabel = isWhisper ? "Whisper / Coach" : "Barge";
  const fac = isWhisper ? "#85" : "*33";

  currentDialCode = `${fac}${extension}`;

  document.getElementById("modalTitle").textContent = `${actionLabel} - ${userLabel}`;
  document.getElementById("modalText").textContent =
    isWhisper
      ? `Dial this code from your Webex Calling line to start supervisor whisper/coaching for extension ${extension}.`
      : `Dial this code from your Webex Calling line to barge into the active call for extension ${extension}.`;

  document.getElementById("modalDialCode").textContent = currentDialCode;
  document.getElementById("modalDialLink").href = `tel:${encodeURIComponent(currentDialCode)}`;
  document.getElementById("actionModal").style.display = "flex";
}

function closeActionModal() {
  document.getElementById("actionModal").style.display = "none";
}

async function copyDialCode() {
  try {
    await navigator.clipboard.writeText(currentDialCode);
    alert(`Copied: ${currentDialCode}`);
  } catch {
    alert(`Dial code: ${currentDialCode}`);
  }
}


function cssStatus(status) {
  if (status === "On Call") return "OnCall";
  if (status === "Not On Call") return "NotOnCall";
  if (status === "Ringing") return "Ringing";
  return "Unknown";
}

function durationSince(value) {
  if (!value) return "N/A";
  const start = new Date(value).getTime();
  if (Number.isNaN(start)) return "N/A";
  const total = Math.max(0, Math.floor((Date.now() - start) / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function populateOrgFilter() {
  const select = document.getElementById("orgFilter");
  const selected = select.value;
  const orgs = [...new Set(agents.map(a => a.org_name || a.org_id || "Unknown Org"))].sort();
  select.innerHTML = `<option value="All">All Organizations</option>` + orgs.map(org => `<option value="${org}">${org}</option>`).join("");
  if ([...select.options].some(o => o.value === selected)) select.value = selected;
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
    document.getElementById("agentBody").innerHTML = `<tr><td class="empty">Could not load /api/agents. Check Render logs.</td></tr>`;
  }
}

async function refreshExtensions() {
  await fetch("/api/refresh-extensions", { method: "POST" });
  await loadAgents();
}

async function refreshOrgs() {
  await fetch("/api/refresh-orgs", { method: "POST" });
  await loadAgents();
}

function renderSummary() {
  document.getElementById("totalCount").textContent = agents.length;
  document.getElementById("ringingCount").textContent = agents.filter(a => a.status === "Ringing").length;
  document.getElementById("onCallCount").textContent = agents.filter(a => a.status === "On Call").length;
  document.getElementById("notOnCallCount").textContent = agents.filter(a => a.status === "Not On Call").length;
}

function renderHeader(columns) {
  const header = document.getElementById("tableHeader");
  header.innerHTML = columns.map(c => `<th draggable="true" data-key="${c.key}">${c.label}</th>`).join("");
  let draggedKey = null;

  header.querySelectorAll("th").forEach(th => {
    th.addEventListener("dragstart", e => {
      draggedKey = th.dataset.key;
      th.classList.add("dragging");
      e.dataTransfer.effectAllowed = "move";
    });

    th.addEventListener("dragend", () => {
      th.classList.remove("dragging");
      header.querySelectorAll("th").forEach(x => x.classList.remove("drag-over"));
    });

    th.addEventListener("dragover", e => {
      e.preventDefault();
      th.classList.add("drag-over");
    });

    th.addEventListener("dragleave", () => th.classList.remove("drag-over"));

    th.addEventListener("drop", e => {
      e.preventDefault();
      th.classList.remove("drag-over");
      const targetKey = th.dataset.key;
      if (!draggedKey || draggedKey === targetKey) return;
      const order = getColumnOrder();
      const from = order.indexOf(draggedKey);
      const to = order.indexOf(targetKey);
      if (from === -1 || to === -1) return;
      order.splice(from, 1);
      order.splice(to, 0, draggedKey);
      setColumnOrder(order);
      renderTable();
    });
  });
}

function renderTable() {
  renderSummary();
  const columns = getOrderedColumns();
  renderHeader(columns);

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
    body.innerHTML = `<tr><td colspan="${columns.length}" class="empty">No users match the current filters.</td></tr>`;
    return;
  }

  body.innerHTML = filtered.map(a => `
    <tr>${columns.map(c => `<td>${c.render(a)}</td>`).join("")}</tr>
  `).join("");
}

async function removeAgent(personId) {
  if (!confirm("Remove this user from the dashboard? This only removes the local row. To delete the Webex webhook, the user should use /oauth/remove/start.")) return;
  await fetch(`/api/agents/${encodeURIComponent(personId)}/remove`, { method: "POST" });
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
    """
    Clears the in-memory org cache and refreshes org display names for stored agents.
    This works best when WEBEX_ADMIN_TOKEN is set. Re-authorizing a user also refreshes
    that user's org name using their OAuth token.
    """
    ORG_NAME_CACHE.clear()
    updated = 0

    with db() as conn:
        rows = conn.execute("SELECT person_id, org_id, org_name FROM agents WHERE org_id IS NOT NULL").fetchall()

        for row in rows:
            org_id = row["org_id"]
            resolved = resolve_org_name(org_id)

            if resolved and resolved != org_id and resolved != row["org_name"]:
                conn.execute(
                    "UPDATE agents SET org_name = ?, updated_at = ? WHERE person_id = ?",
                    (resolved, now_iso(), row["person_id"]),
                )
                updated += 1

    return {"message": "organization refresh complete", "updated": updated}


@app.get("/api/agents")
def api_agents():
    with db() as conn:
        rows = conn.execute("SELECT * FROM agents").fetchall()

    agents = [dict(row) for row in rows]
    priority = {"Ringing": 0, "On Call": 1, "Unknown": 2, "Not On Call": 3}
    agents.sort(key=lambda a: (
        a.get("org_name") or "",
        priority.get(a.get("status"), 9),
        a.get("email") or a.get("display_name") or "",
    ))

    return {"count": len(agents), "agents": agents}


@app.post("/api/refresh-extensions")
def api_refresh_extensions():
    updated = 0
    with db() as conn:
        rows = conn.execute("SELECT person_id FROM agents").fetchall()
        for row in rows:
            extension = resolve_user_extension(row["person_id"])
            if extension:
                conn.execute(
                    "UPDATE agents SET extension = ?, updated_at = ? WHERE person_id = ?",
                    (extension, now_iso(), row["person_id"]),
                )
                updated += 1
    return {"message": "extension refresh complete", "updated": updated}


@app.post("/api/agents/{person_id}/remove")
def api_remove_agent(person_id: str):
    remove_agent_from_dashboard(person_id)
    return {"message": "agent removed from dashboard"}


@app.post("/api/reset")
def api_reset():
    with db() as conn:
        conn.execute("DELETE FROM agents")
        conn.execute("DELETE FROM events")
    return {"message": "reset complete"}


@app.get("/oauth/start")
def oauth_start():
    if not WEBEX_CLIENT_ID:
        return HTMLResponse("Missing WEBEX_CLIENT_ID in Render environment variables.", status_code=200)

    if not WEBEX_REDIRECT_URI:
        return HTMLResponse("Missing WEBEX_REDIRECT_URI in Render environment variables.", status_code=200)

    auth_url = build_webex_authorize_url(WEBEX_REDIRECT_URI, "webex-calling-supervisor-dashboard")
    return RedirectResponse(auth_url, status_code=302)


@app.get("/oauth/callback")
def oauth_callback(request: Request):
    code = request.query_params.get("code")

    if not code:
        error = request.query_params.get("error")
        error_description = request.query_params.get("error_description")
        return HTMLResponse(f"""
        <html>
          <body style="font-family: Arial; padding: 40px;">
            <h2>Webex Authorization Failed</h2>
            <p>Webex did not return an authorization code.</p>
            <p><strong>Error:</strong> {error or "N/A"}</p>
            <p><strong>Description:</strong> {error_description or "N/A"}</p>
            <p>Start over from /oauth/start.</p>
          </body>
        </html>
        """, status_code=400)

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
        return HTMLResponse(f"""
        <html>
          <body style="font-family: Arial; padding: 40px;">
            <h2>Webex Token Exchange Failed</h2>
            <p>{token_response.text}</p>
            <p>Start over from /oauth/start. Do not refresh the callback URL.</p>
          </body>
        </html>
        """, status_code=400)

    access_token = token_response.json().get("access_token")
    me = get_me(access_token)
    webhook = create_call_status_webhook(access_token)

    if me:
        upsert_agent_from_oauth(me, webhook, access_token)

    return HTMLResponse("""
    <html>
      <head>
        <title>Webex User Connected</title>
        <style>
          body { font-family: Arial, sans-serif; background: #eef3f8; padding: 40px; color: #101828; }
          .card { background: white; padding: 24px; border-radius: 16px; max-width: 720px; box-shadow: 0 8px 18px rgba(15,23,42,.08); }
          .success { color: #166534; font-weight: 800; }
          .muted { color: #667085; font-size: 14px; }
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


@app.get("/oauth/remove/start")
def oauth_remove_start():
    if not WEBEX_CLIENT_ID:
        return HTMLResponse("Missing WEBEX_CLIENT_ID in Render environment variables.", status_code=200)

    if not WEBEX_REDIRECT_URI:
        return HTMLResponse("Missing WEBEX_REDIRECT_URI in Render environment variables.", status_code=200)

    remove_redirect_uri = WEBEX_REDIRECT_URI.replace("/oauth/callback", "/oauth/remove/callback")
    auth_url = build_webex_authorize_url(remove_redirect_uri, "webex-calling-disconnect")
    return RedirectResponse(auth_url, status_code=302)


@app.get("/oauth/remove/callback")
def oauth_remove_callback(request: Request):
    code = request.query_params.get("code")

    if not code:
        return HTMLResponse("Missing authorization code. Start from /oauth/remove/start.", status_code=400)

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
        return HTMLResponse(f"Token exchange failed: {token_response.text}", status_code=400)

    access_token = token_response.json().get("access_token")
    me = get_me(access_token)
    delete_result = delete_call_status_webhooks_for_user(access_token)

    if me and me.get("id"):
        remove_agent_from_dashboard(me.get("id"))

    return HTMLResponse(f"""
    <html>
      <body style="font-family: Arial; background: #eef3f8; padding: 40px;">
        <div style="background: white; padding: 24px; border-radius: 16px; max-width: 720px;">
          <h2 style="color: #166534;">Disconnected Successfully</h2>
          <p>Your Webex Calling status connection has been removed.</p>
          <p>Deleted webhook count: {len(delete_result.get("deleted", []))}</p>
          <p>You can close this browser tab now.</p>
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
