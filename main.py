import os
import json
import sqlite3
import secrets
import time
import threading
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI(title="Webex Attendant Console")

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
SCOPES = "spark:calls_read spark:calls_write spark:webhooks_write spark:webhooks_read spark:people_read spark-admin:organizations_read spark-admin:people_read"

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "attendant_console.db"

# Limit stale live-call states so the dashboard does not show weekend-long timers.
# Override with STALE_STATUS_AFTER_SECONDS if you want a different limit.
STALE_STATUS_AFTER_SECONDS = int(os.getenv("STALE_STATUS_AFTER_SECONDS", "86400"))

# Keep webhook history from growing forever on small Render instances.
# MAX_EVENT_ROWS keeps only the newest rows; WEBHOOK_PAYLOAD_MAX_CHARS limits
# how much raw webhook JSON is stored per event for troubleshooting.
MAX_EVENT_ROWS = int(os.getenv("MAX_EVENT_ROWS", "1000"))
WEBHOOK_PAYLOAD_MAX_CHARS = int(os.getenv("WEBHOOK_PAYLOAD_MAX_CHARS", "4000"))

# Automatically dump stored webhook activity on a timer. This clears only the
# events table used by Recent Webex Activity; it does not remove dashboard users
# or OAuth sessions. Default is every 1 hour.
AUTO_CLEAR_EVENTS_ENABLED = os.getenv("AUTO_CLEAR_EVENTS_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
AUTO_CLEAR_EVENTS_EVERY_SECONDS = int(os.getenv("AUTO_CLEAR_EVENTS_EVERY_SECONDS", "3600"))
AUTO_CLEAR_EVENTS_VACUUM = os.getenv("AUTO_CLEAR_EVENTS_VACUUM", "false").strip().lower() in {"1", "true", "yes", "on"}

# DND controls use the Webex Calling Person Settings API. The default endpoint
# is the standard user feature path; override WEBEX_DND_ENDPOINT_TEMPLATE if
# your tenant/API version expects a different path. Use {person_id} in the template.
WEBEX_DND_ENDPOINT_TEMPLATE = os.getenv(
    "WEBEX_DND_ENDPOINT_TEMPLATE",
    "https://webexapis.com/v1/people/{person_id}/features/doNotDisturb",
)
WEBEX_DND_DEFAULT_RING_REMINDER = os.getenv("WEBEX_DND_DEFAULT_RING_REMINDER", "false").strip().lower() in {"1", "true", "yes", "on"}

ORG_NAME_CACHE: Dict[str, str] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        normalized = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def should_mark_status_stale(agent: Dict[str, Any]) -> bool:
    status = agent.get("status")
    if status not in {"Ringing", "On Call", "Outbound", "Unknown"}:
        return False

    started = parse_iso_datetime(agent.get("state_started_at") or agent.get("updated_at"))
    if not started:
        return False

    age_seconds = (datetime.now(timezone.utc) - started).total_seconds()
    return age_seconds > STALE_STATUS_AFTER_SECONDS


def apply_dashboard_status_rules(agent: Dict[str, Any]) -> Dict[str, Any]:
    if agent.get("status") == "Unknown":
        agent["status"] = "Outbound"

    if should_mark_status_stale(agent):
        agent["original_status"] = agent.get("status")
        agent["status"] = "Needs Refresh"
        agent["is_stale"] = True
    else:
        agent["is_stale"] = False

    return agent


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def compact_payload(event: Dict[str, Any]) -> str:
    """Store enough webhook JSON for troubleshooting without bloating SQLite."""
    try:
        text = json.dumps(event, separators=(",", ":"))
    except Exception:
        text = str(event)

    if WEBHOOK_PAYLOAD_MAX_CHARS > 0 and len(text) > WEBHOOK_PAYLOAD_MAX_CHARS:
        return text[:WEBHOOK_PAYLOAD_MAX_CHARS] + "...[truncated]"
    return text


def cleanup_event_history(conn: sqlite3.Connection):
    """Keep only the newest MAX_EVENT_ROWS webhook events."""
    if MAX_EVENT_ROWS <= 0:
        return

    conn.execute("""
        DELETE FROM events
        WHERE id NOT IN (
            SELECT id FROM events
            ORDER BY id DESC
            LIMIT ?
        )
    """, (MAX_EVENT_ROWS,))


def clear_webhook_event_history(vacuum: bool = False) -> Dict[str, Any]:
    """Clear webhook activity history without removing users or OAuth sessions."""
    with db() as conn:
        before = conn.execute("SELECT COUNT(*) AS count FROM events").fetchone()["count"]
        conn.execute("DELETE FROM events")

    # VACUUM can briefly lock the SQLite database, so the hourly job leaves this
    # off by default. Enable AUTO_CLEAR_EVENTS_VACUUM=true if you need disk-space
    # compaction after each scheduled clear.
    if vacuum:
        with db() as conn:
            conn.execute("VACUUM")

    return {"events_before": before, "events_after": 0, "vacuum": vacuum}


def auto_clear_events_loop():
    while True:
        time.sleep(max(60, AUTO_CLEAR_EVENTS_EVERY_SECONDS))
        try:
            result = clear_webhook_event_history(vacuum=AUTO_CLEAR_EVENTS_VACUUM)
            print(
                "Auto-cleared webhook event history: "
                f"{result['events_before']} rows removed; "
                f"vacuum={result['vacuum']}"
            )
        except Exception as exc:
            print(f"Auto-clear webhook event history failed: {exc}")


@app.on_event("startup")
def start_auto_clear_events_timer():
    if not AUTO_CLEAR_EVENTS_ENABLED:
        print("Auto-clear webhook event history is disabled.")
        return

    if AUTO_CLEAR_EVENTS_EVERY_SECONDS <= 0:
        print("Auto-clear webhook event history is disabled because interval <= 0.")
        return

    if getattr(app.state, "auto_clear_events_started", False):
        return

    app.state.auto_clear_events_started = True
    thread = threading.Thread(target=auto_clear_events_loop, daemon=True)
    thread.start()
    print(f"Auto-clear webhook event history started every {AUTO_CLEAR_EVENTS_EVERY_SECONDS} seconds.")


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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_id TEXT PRIMARY KEY,
                person_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        agent_columns = {row["name"] for row in conn.execute("PRAGMA table_info(agents)").fetchall()}
        for column_name in ["extension", "org_id", "org_name", "access_token", "refresh_token", "token_expires_at", "dnd_enabled", "dnd_ring_reminder"]:
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

    # Webex outbound call events can arrive without a familiar state/eventType.
    # Instead of showing these as Unknown in the attendant console, treat the
    # active, non-ended fallback as Outbound.
    return "Outbound"


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


def token_expiry_from_response(token_json: Dict[str, Any]) -> str:
    """Return epoch seconds for when the access token expires."""
    try:
        expires_in = int(token_json.get("expires_in") or 0)
    except Exception:
        expires_in = 0

    # Give ourselves a 60-second safety buffer.
    return str(int(time.time()) + max(0, expires_in - 60))


def upsert_agent_from_oauth(me: Dict[str, Any], webhook: Dict[str, Any], user_access_token: str, token_json: Optional[Dict[str, Any]] = None):
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
    token_json = token_json or {}
    access_token = token_json.get("access_token") or user_access_token
    refresh_token = token_json.get("refresh_token")
    token_expires_at = token_expiry_from_response(token_json) if token_json else None

    with db() as conn:
        existing = conn.execute(
            "SELECT person_id FROM agents WHERE person_id = ?",
            (person_id,),
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE agents
                SET email = ?, display_name = ?, extension = COALESCE(?, extension),
                    org_id = ?, org_name = ?, webhook_id = ?, updated_at = ?,
                    access_token = COALESCE(?, access_token),
                    refresh_token = COALESCE(?, refresh_token),
                    token_expires_at = COALESCE(?, token_expires_at)
                WHERE person_id = ?
            """, (
                email, display_name, extension, org_id, org_name, webhook.get("id"), ts,
                access_token, refresh_token, token_expires_at, person_id,
            ))
        else:
            conn.execute("""
                INSERT INTO agents (
                    person_id, email, display_name, extension, org_id, org_name,
                    status, state_started_at, updated_at, webhook_id,
                    access_token, refresh_token, token_expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'Not On Call', ?, ?, ?, ?, ?, ?)
            """, (
                person_id, email, display_name, extension, org_id, org_name,
                ts, ts, webhook.get("id"), access_token, refresh_token, token_expires_at,
            ))


def create_user_session(person_id: str) -> str:
    session_id = secrets.token_urlsafe(32)
    ts = now_iso()
    with db() as conn:
        conn.execute(
            "INSERT INTO user_sessions (session_id, person_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (session_id, person_id, ts, ts),
        )
    return session_id


def person_id_from_session(session_id: Optional[str]) -> Optional[str]:
    if not session_id:
        return None
    ts = now_iso()
    with db() as conn:
        row = conn.execute(
            "SELECT person_id FROM user_sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if row:
            # Keep active browser sessions fresh so users do not have to re-authenticate constantly.
            conn.execute(
                "UPDATE user_sessions SET updated_at = ? WHERE session_id = ?",
                (ts, session_id),
            )
    return row["person_id"] if row else None


def is_authenticated_agent_row(row: Any) -> bool:
    """Return True only when the row represents a user who has completed OAuth.

    Webhook-only events sometimes provide only a Webex personId. Those placeholder
    rows are useful for history/troubleshooting, but they should not appear in the
    attendant user table as a long unreadable ID.
    """
    if not row:
        return False

    try:
        access_token = row["access_token"]
    except Exception:
        access_token = None

    try:
        refresh_token = row["refresh_token"]
    except Exception:
        refresh_token = None

    try:
        email = row["email"]
    except Exception:
        email = None

    try:
        person_id = row["person_id"]
    except Exception:
        person_id = None

    # Newer rows authenticated through this app have tokens. Older authenticated
    # rows may only have a real email from /people/me, so keep those visible too.
    return bool(access_token or refresh_token or (email and email != person_id and "@" in str(email)))


def get_user_token_for_call_control(person_id: str) -> str:
    with db() as conn:
        row = conn.execute(
            "SELECT access_token, refresh_token, token_expires_at FROM agents WHERE person_id = ?",
            (person_id,),
        ).fetchone()

    if not row or not row["access_token"]:
        raise HTTPException(status_code=401, detail="No stored Webex token found. Open /oauth/start again as this user.")

    access_token = row["access_token"]
    refresh_token = row["refresh_token"]

    try:
        expires_at = int(row["token_expires_at"] or "0")
    except Exception:
        expires_at = 0

    if expires_at and expires_at > int(time.time()):
        return access_token

    if not refresh_token:
        raise HTTPException(status_code=401, detail="Webex token expired and no refresh token is stored. Open /oauth/start again.")

    refresh_response = requests.post(
        "https://webexapis.com/v1/access_token",
        data={
            "grant_type": "refresh_token",
            "client_id": WEBEX_CLIENT_ID,
            "client_secret": WEBEX_CLIENT_SECRET,
            "refresh_token": refresh_token,
        },
        timeout=20,
    )

    if refresh_response.status_code >= 400:
        raise HTTPException(
            status_code=401,
            detail=f"Unable to refresh Webex token. Reconnect with /oauth/start. Webex said: {refresh_response.text}",
        )

    token_json = refresh_response.json()
    new_access_token = token_json.get("access_token")
    new_refresh_token = token_json.get("refresh_token") or refresh_token
    new_expires_at = token_expiry_from_response(token_json)

    with db() as conn:
        conn.execute(
            """
            UPDATE agents
            SET access_token = ?, refresh_token = ?, token_expires_at = ?, updated_at = ?
            WHERE person_id = ?
            """,
            (new_access_token, new_refresh_token, new_expires_at, now_iso(), person_id),
        )

    return new_access_token


def webex_request(method: str, url: str, access_token: str, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {}) or {}
    headers.update({
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    return requests.request(method, url, headers=headers, timeout=20, **kwargs)


def list_my_active_calls(access_token: str) -> Dict[str, Any]:
    """
    Try the classic user-level Call Controls list endpoint first, then the newer
    self-scoped Members Me path if the org/API version expects that shape.
    """
    endpoints = [
        "https://webexapis.com/v1/telephony/calls",
        "https://webexapis.com/v1/telephony/calls/members/me/calls",
    ]

    last_error = None
    for url in endpoints:
        response = webex_request("GET", url, access_token)
        if response.status_code < 400:
            data = response.json() if response.text else {}
            items = data.get("items") if isinstance(data, dict) else None
            if items is None and isinstance(data, list):
                items = data
            return {"endpoint": url, "items": items or []}
        last_error = f"{response.status_code}: {response.text}"

    raise HTTPException(status_code=502, detail=f"Unable to list active Webex calls. Last Webex response: {last_error}")


def pick_transferable_call(calls: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    preferred_states = {"connected", "held", "remoteheld", "remoteHeld"}
    fallback_states = {"connecting", "alerting"}

    for call in calls:
        state = str(call.get("state") or call.get("status") or "").strip()
        if state in preferred_states or state.lower() in {s.lower() for s in preferred_states}:
            return call

    for call in calls:
        state = str(call.get("state") or call.get("status") or "").strip()
        if state in fallback_states or state.lower() in {s.lower() for s in fallback_states}:
            return call

    return calls[0] if len(calls) == 1 else None


def extract_call_id(call: Dict[str, Any]) -> Optional[str]:
    return call.get("id") or call.get("callId") or call.get("call_id")


def transfer_my_call(access_token: str, call_id: str, destination: str) -> Dict[str, Any]:
    body = {"callId1": call_id, "destination": destination}
    endpoints = [
        "https://webexapis.com/v1/telephony/calls/transfer",
        "https://webexapis.com/v1/telephony/calls/members/me/transfer",
    ]

    last_error = None
    for url in endpoints:
        response = webex_request("POST", url, access_token, json=body)
        if response.status_code in (200, 201, 202, 204):
            payload = response.json() if response.text else {}
            return {"success": True, "endpoint": url, "status_code": response.status_code, "response": payload}
        last_error = f"{response.status_code}: {response.text}"

    raise HTTPException(status_code=502, detail=f"Transfer failed. Last Webex response: {last_error}")


def dial_from_my_webex(access_token: str, destination: str) -> Dict[str, Any]:
    """Place a new call from the signed-in user's Webex Calling account."""
    bodies = [
        {"destination": destination},
        {"phoneNumber": destination},
        {"address": destination},
    ]
    endpoints = [
        "https://webexapis.com/v1/telephony/calls/dial",
        "https://webexapis.com/v1/telephony/calls/members/me/dial",
        "https://webexapis.com/v1/telephony/calls/members/me/actions/dial",
    ]

    last_error = None
    for url in endpoints:
        for body in bodies:
            response = webex_request("POST", url, access_token, json=body)
            if response.status_code in (200, 201, 202, 204):
                payload = response.json() if response.text else {}
                return {
                    "success": True,
                    "endpoint": url,
                    "status_code": response.status_code,
                    "request_body": body,
                    "response": payload,
                }
            last_error = f"{response.status_code}: {response.text}"

    raise HTTPException(status_code=502, detail=f"Call failed. Last Webex response: {last_error}")



def bool_to_db(value: Optional[bool]) -> Optional[str]:
    if value is None:
        return None
    return "1" if bool(value) else "0"


def db_to_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def get_token_for_dnd(target_person_id: str) -> str:
    """Use the admin token for changing other users, with a self-token fallback."""
    if WEBEX_ADMIN_TOKEN:
        return WEBEX_ADMIN_TOKEN
    return get_user_token_for_call_control(target_person_id)


def dnd_endpoint_candidates(person_id: str) -> list[str]:
    endpoint = WEBEX_DND_ENDPOINT_TEMPLATE.format(person_id=person_id)
    candidates = [endpoint]

    # Keep these fallback candidates for tenants/API versions that expose the
    # feature path under telephony/config. They are tried only if the first path fails.
    fallback = f"https://webexapis.com/v1/telephony/config/people/{person_id}/features/doNotDisturb"
    if fallback not in candidates:
        candidates.append(fallback)

    return candidates


def add_org_params(url: str, org_id: Optional[str]) -> tuple[str, Dict[str, str]]:
    params: Dict[str, str] = {}
    if org_id:
        params["orgId"] = org_id
    return url, params


def read_person_dnd_settings(person_id: str, org_id: Optional[str] = None) -> Dict[str, Any]:
    token = get_token_for_dnd(person_id)
    last_error = None

    for url in dnd_endpoint_candidates(person_id):
        url, params = add_org_params(url, org_id)
        response = webex_request("GET", url, token, params=params)
        if response.status_code < 400:
            data = response.json() if response.text else {}
            enabled = data.get("enabled")
            ring_reminder = data.get("ringSplashEnabled", data.get("ringReminderEnabled"))
            return {
                "success": True,
                "endpoint": url,
                "enabled": bool(enabled),
                "ringReminderEnabled": bool(ring_reminder) if ring_reminder is not None else False,
                "response": data,
            }
        last_error = f"{response.status_code}: {response.text}"

    raise HTTPException(status_code=502, detail=f"Unable to read DND settings from Webex. Last Webex response: {last_error}")


def set_person_dnd_settings(person_id: str, org_id: Optional[str], enabled: bool, ring_reminder: Optional[bool] = None) -> Dict[str, Any]:
    token = get_token_for_dnd(person_id)
    ring_value = WEBEX_DND_DEFAULT_RING_REMINDER if ring_reminder is None else bool(ring_reminder)

    payloads = [
        {"enabled": bool(enabled), "ringSplashEnabled": ring_value},
        {"enabled": bool(enabled), "ringReminderEnabled": ring_value},
        {"enabled": bool(enabled)},
    ]

    last_error = None
    for url in dnd_endpoint_candidates(person_id):
        url, params = add_org_params(url, org_id)
        for body in payloads:
            response = webex_request("PUT", url, token, params=params, json=body)
            if response.status_code in (200, 201, 202, 204):
                data = response.json() if response.text else {}
                with db() as conn:
                    conn.execute(
                        """
                        UPDATE agents
                        SET dnd_enabled = ?, dnd_ring_reminder = ?, updated_at = ?
                        WHERE person_id = ?
                        """,
                        (bool_to_db(enabled), bool_to_db(ring_value), now_iso(), person_id),
                    )
                return {
                    "success": True,
                    "endpoint": url,
                    "enabled": bool(enabled),
                    "ringReminderEnabled": ring_value,
                    "response": data,
                }
            last_error = f"{response.status_code}: {response.text}"

    raise HTTPException(status_code=502, detail=f"Unable to update DND settings in Webex. Last Webex response: {last_error}")


def refresh_dnd_for_agent(person_id: str, org_id: Optional[str]) -> Optional[Dict[str, Any]]:
    try:
        result = read_person_dnd_settings(person_id, org_id)
    except Exception as exc:
        print(f"DND refresh failed for {person_id}: {exc}")
        return None

    with db() as conn:
        conn.execute(
            """
            UPDATE agents
            SET dnd_enabled = ?, dnd_ring_reminder = ?, updated_at = ?
            WHERE person_id = ?
            """,
            (
                bool_to_db(result.get("enabled")),
                bool_to_db(result.get("ringReminderEnabled")),
                now_iso(),
                person_id,
            ),
        )
    return result


def remove_agent_from_dashboard(person_id: str):
    with db() as conn:
        conn.execute("DELETE FROM agents WHERE person_id = ?", (person_id,))
        conn.execute("DELETE FROM events WHERE person_id = ?", (person_id,))
        conn.execute("DELETE FROM user_sessions WHERE person_id = ?", (person_id,))


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
            # Store webhook-only users as hidden placeholders. Do not use the raw
            # Webex personId as the email/display name because it clutters the UI
            # before the user has authenticated through /oauth/start.
            conn.execute("""
                INSERT INTO agents (
                    person_id, email, display_name, extension, org_id, org_name, status,
                    webex_state, event_type, call_id, call_session_id,
                    remote_name, remote_number, remote_call_type,
                    state_started_at, updated_at, webhook_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                person_id, None, None, extension, org_id, org_name, new_status,
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
            data.get("callId"), data.get("callSessionId"), compact_payload(event), ts,
        ))

        cleanup_event_history(conn)

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
        <title>Webex Attendant Console Connector</title>
        <style>
          body { font-family: Arial, sans-serif; background: #eef3f8; padding: 40px; color: #101828; }
          .card { background: white; padding: 24px; border-radius: 16px; max-width: 720px; box-shadow: 0 8px 18px rgba(15,23,42,.08); }
          a.button { display: inline-block; margin-top: 12px; background: #2563eb; color: white; text-decoration: none; padding: 10px 14px; border-radius: 10px; }
          .muted { color: #667085; font-size: 14px; }
        </style>
      </head>
      <body>
        <div class="card">
          <h2>Webex Attendant Console Connector</h2>
          <p>This page connects a Webex Calling user to the attendant console.</p>
          <a class="button" href="/attendantconsole">Open Attendant Console</a>
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
        "stale_status_after_seconds": STALE_STATUS_AFTER_SECONDS,
        "max_event_rows": MAX_EVENT_ROWS,
        "webhook_payload_max_chars": WEBHOOK_PAYLOAD_MAX_CHARS,
        "auto_clear_events_enabled": AUTO_CLEAR_EVENTS_ENABLED,
        "auto_clear_events_every_seconds": AUTO_CLEAR_EVENTS_EVERY_SECONDS,
        "auto_clear_events_vacuum": AUTO_CLEAR_EVENTS_VACUUM,
        "dnd_endpoint_template": WEBEX_DND_ENDPOINT_TEMPLATE,
        "dnd_default_ring_reminder": WEBEX_DND_DEFAULT_RING_REMINDER,
    }


@app.get("/supervisor")
def supervisor_redirect():
    return RedirectResponse("/attendantconsole", status_code=302)


@app.get("/attendantconsole", response_class=HTMLResponse)
def attendant_console_dashboard():
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Webex Attendant Console</title>
  <style>
    :root {
      --bg: #eef4fb; --panel: #ffffff; --text: #102033; --muted: #5d6f89;
      --border: #c7d5e8; --blue: #159bd3; --navy: #0d2a6d; --navy-dark: #071b49;
      --green-bg: #dff8ee; --green-text: #08734f;
      --red-bg: #fee2e2; --red-text: #991b1b; --yellow-bg: #fff1c7; --yellow-text: #8a5a00;
      --gray-bg: #e8eef7; --gray-text: #31435f;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Arial, Helvetica, sans-serif; background: var(--bg); color: var(--text); }
    header {
      background: linear-gradient(135deg, var(--navy-dark), var(--navy));
      color: white;
      padding: 22px 30px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 24px;
      border-bottom: 4px solid #159bd3;
    }
    .header-text h1 { margin: 0; font-size: 28px; }
    .header-text p { margin: 7px 0 0; color: #d8ecff; font-size: 14px; }
    .header-logo-wrap {
      background: transparent;
      border-radius: 0;
      padding: 0;
      box-shadow: none;
      flex: 0 0 auto;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .header-logo {
      width: 310px;
      height: auto;
      max-height: 86px;
      object-fit: contain;
      display: block;
    }
    main { width: 100%; max-width: none; margin: 0; padding: 24px 32px; }
    .toolbar { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; width: 100%; }
    .toolbar-left { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    input, select { border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; font-size: 14px; min-width: 240px; background: white; }
    button, a.button { border: none; border-radius: 10px; background: var(--blue); color: white; padding: 10px 14px; font-size: 14px; cursor: pointer; text-decoration: none; display: inline-block; }
    button.secondary { background: #244264; }
    button.small { padding: 7px 10px; font-size: 12px; }
    .summary { display: grid; grid-template-columns: repeat(5, minmax(160px, 1fr)); gap: 14px; margin-bottom: 16px; width: 100%; }
    .summary-card { background: var(--panel); border: 1px solid var(--border); border-radius: 16px; padding: 16px; box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06); }
    .summary-card .label { color: var(--muted); font-size: 13px; }
    .summary-card .value { font-size: 30px; font-weight: 800; margin-top: 4px; }
    .activity-panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 18px;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06);
      margin-top: 34px;
      margin-bottom: 16px;
      position: relative;
    }
    .activity-panel::before {
      content: "";
      position: absolute;
      top: -18px;
      left: 0;
      right: 0;
      height: 1px;
      background: #d0d5dd;
    }
    .activity-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 10px;
    }
    .activity-header h2 { margin: 0; font-size: 18px; }
    .activity-list { display: grid; gap: 8px; }
    .activity-item {
      display: grid;
      grid-template-columns: 150px 1fr 130px 1fr;
      gap: 10px;
      align-items: center;
      padding: 10px 12px;
      background: #f8fafc;
      border: 1px solid #eef2f7;
      border-radius: 12px;
      font-size: 13px;
    }
    .activity-time { color: var(--muted); font-weight: 700; }
    .activity-user { font-weight: 800; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .activity-meta { color: #475569; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .org-row td {
      background: #e6f6fd;
      color: #0d2a6d;
      font-weight: 900;
      text-transform: uppercase;
      letter-spacing: .04em;
      font-size: 12px;
      border-top: 1px solid #b6e7f8;
      border-bottom: 1px solid #b6e7f8;
    }
    .org-count { color: #159bd3; font-weight: 700; text-transform: none; letter-spacing: 0; margin-left: 8px; }
    .table-wrap {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow-x: auto;
      overflow-y: hidden;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06);
      scrollbar-width: auto;
      width: 100%;
      max-width: 100%;
    }

    .table-wrap::-webkit-scrollbar {
      height: 14px;
    }

    .table-wrap::-webkit-scrollbar-track {
      background: #e5e7eb;
      border-radius: 999px;
    }

    .table-wrap::-webkit-scrollbar-thumb {
      background: #94a3b8;
      border-radius: 999px;
      border: 3px solid #e5e7eb;
    }

    .table-wrap::-webkit-scrollbar-thumb:hover {
      background: #64748b;
    }
    table {
      width: max-content;
      min-width: 1750px;
      border-collapse: collapse;
      font-size: 14px;
      table-layout: auto;
    }
    thead { background: #f8fafc; }
    th {
      text-align: left;
      padding: 14px 18px;
      color: #475569;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      border-bottom: 1px solid var(--border);
      white-space: nowrap;
      user-select: none;
      min-width: 125px;
    }
    th[draggable="true"] { cursor: grab; }
    th.dragging { opacity: .45; }
    th.drag-over { outline: 2px dashed var(--blue); outline-offset: -4px; }
    td {
      padding: 14px 18px;
      border-bottom: 1px solid #eef2f7;
      vertical-align: middle;
      white-space: nowrap;
    }
    tr:last-child td { border-bottom: none; }
    tbody tr:hover { background: #f8fafc; }
    .email {
      font-weight: 800;
      color: #0f172a;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      max-width: 340px;
      display: block;
    }

    .cell-clip {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      max-width: 360px;
      display: block;
    }
    .pill { display: inline-flex; align-items: center; border-radius: 999px; padding: 7px 11px; font-weight: 800; font-size: 12px; white-space: nowrap; }
    .Ringing { background: var(--yellow-bg); color: var(--yellow-text); }
    .OnCall { background: var(--red-bg); color: var(--red-text); }
    .NotOnCall { background: var(--green-bg); color: var(--green-text); }
    .Unknown, .Outbound, .NeedsRefresh { background: var(--gray-bg); color: var(--gray-text); }
    .DndOn { background: var(--red-bg); color: var(--red-text); }
    .DndOff { background: var(--green-bg); color: var(--green-text); }
    .duration { font-weight: 800; color: #0f172a; white-space: nowrap; }
    .empty { padding: 36px; text-align: center; color: var(--muted); }

    .scroll-hint {
      color: var(--muted);
      font-size: 13px;
      margin: 0 0 8px 2px;
    }

    .transfer-btn {
      border: none;
      border-radius: 10px;
      padding: 8px 13px;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
      background: #dff4fb;
      color: #0d6f99;
    }
    .transfer-btn:disabled {
      background: #e5e7eb;
      color: #9ca3af;
      cursor: not-allowed;
    }
    .dnd-actions { display: flex; align-items: center; gap: 8px; white-space: nowrap; }
    .dnd-status {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 7px 10px;
      font-size: 12px;
      font-weight: 900;
      min-width: 82px;
      border: 1px solid transparent;
    }
    .dnd-status.on { background: #fee2e2; color: #991b1b; border-color: #fecaca; }
    .dnd-status.off { background: #dcfce7; color: #166534; border-color: #bbf7d0; }
    .dnd-status.unknown { background: #e5e7eb; color: #374151; border-color: #d1d5db; }
    .dnd-btn {
      border: none;
      border-radius: 10px;
      padding: 8px 11px;
      font-size: 12px;
      font-weight: 900;
      cursor: pointer;
      white-space: nowrap;
    }
    .dnd-btn.on { background: #fee2e2; color: #991b1b; }
    .dnd-btn.off { background: #dcfce7; color: #166534; }
    .dnd-btn:disabled { background: #e5e7eb; color: #9ca3af; cursor: not-allowed; }
    .call-btn, .reset-status-btn {
      border: none;
      border-radius: 10px;
      padding: 8px 13px;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
      white-space: nowrap;
    }
    .call-btn { background: #e0f2fe; color: #075985; }
    .reset-status-btn { background: #f1f5f9; color: #334155; }
    .call-btn:disabled, .reset-status-btn:disabled {
      background: #e5e7eb;
      color: #9ca3af;
      cursor: not-allowed;
    }
    .transfer-status {
      display: none;
      margin: 0 0 16px 0;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid #cbd5e1;
      background: #f8fafc;
      color: #334155;
      font-weight: 700;
    }
    .transfer-status.success {
      display: block;
      border-color: #86efac;
      background: #ecfdf3;
      color: #166534;
    }
    .transfer-status.error {
      display: block;
      border-color: #fecaca;
      background: #fef2f2;
      color: #991b1b;
    }
    .transfer-status.info {
      display: block;
      border-color: #bfdbfe;
      background: #eff6ff;
      color: #1d4ed8;
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
      font-size: 30px;
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
      header { align-items: flex-start; }
      .header-logo-wrap { padding: 6px; border-radius: 14px; }
      .header-logo { width: 210px; height: auto; max-height: 58px; }
      main { padding: 14px; }
      .summary { grid-template-columns: repeat(2, 1fr); }
      .activity-item { grid-template-columns: 1fr; gap: 4px; }
      .table-wrap { overflow-x: auto; }
      table { min-width: 1750px; }
      input, select, button, a.button { width: 100%; }
      .toolbar, .toolbar-left { width: 100%; align-items: stretch; }
    }
  </style>
</head>
<body>
<header>
  <div class="header-text">
    <h1>Webex Attendant Console</h1>
    <p>Monitor Webex Calling users by organization, current state, duration, and extension.</p>
  </div>
  <div class="header-logo-wrap">
    <img class="header-logo" src="data:image/png;base64,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" alt="Call Pros logo" />
  </div>
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
        <option value="Outbound">Outbound</option>
        <option value="Needs Refresh">Needs Refresh</option>
      </select>
      <button onclick="loadAgents()">Refresh</button>
      <button class="secondary" onclick="refreshExtensions()">Refresh Extensions</button>
      <button class="secondary" onclick="refreshDnd()">Refresh DND</button>
      <button class="secondary" onclick="refreshOrgs()">Refresh Orgs</button>
      <button class="secondary" onclick="resetColumnOrder()">Reset Columns</button>
    </div>
  </div>

  <div id="transferStatus" class="transfer-status"></div>

  <section class="summary">
    <div class="summary-card"><div class="label">Total Users</div><div class="value" id="totalCount">0</div></div>
    <div class="summary-card"><div class="label">Ringing</div><div class="value" id="ringingCount">0</div></div>
    <div class="summary-card"><div class="label">On Call</div><div class="value" id="onCallCount">0</div></div>
    <div class="summary-card"><div class="label">Not On Call</div><div class="value" id="notOnCallCount">0</div></div>
    <div class="summary-card"><div class="label">Needs Refresh</div><div class="value" id="staleCount">0</div></div>
  </section>

  <div class="scroll-hint">Users are grouped by organization. Tip: use the horizontal scrollbar at the bottom of the table to see all columns.</div>
  <div class="table-wrap">
    <table>
      <thead><tr id="tableHeader"></tr></thead>
      <tbody id="agentBody"><tr><td class="empty">Loading...</td></tr></tbody>
    </table>
  </div>

  <section class="activity-panel">
    <div class="activity-header">
      <h2>Recent Webex Activity</h2>
      <button class="secondary small" onclick="loadEvents()">Refresh Activity</button>
    </div>
    <div id="activityList" class="activity-list">
      <div class="empty">Loading recent webhook activity...</div>
    </div>
  </section>

  <div id="transferModal" class="modal-backdrop">
    <div class="modal">
      <h2 id="transferModalTitle">Transfer Call</h2>
      <p id="transferModalText"></p>
      <div id="transferExtension" class="dial-code"></div>
      <div class="modal-actions">
        <a id="transferDialLink" class="button" href="#">Open Dialer</a>
        <button onclick="copyTransferExtension()">Copy Extension</button>
        <button class="secondary" onclick="closeTransferModal()">Close</button>
      </div>
    </div>
  </div>

</main>

<script>
let agents = [];
let dndChangeInProgress = false;
let recentEvents = [];

const DEFAULT_COLUMNS = [
  { key: "email", label: "User Email", render: a => `<div class="email" title="${a.email || "Unknown"}">${a.email || "Unknown"}</div>` },
  { key: "organization", label: "Organization", render: a => {
    const org = a.org_name || a.org_id || "Unknown Org";
    return `<span class="cell-clip" title="${org}">${org}</span>`;
  } },
  { key: "extension", label: "Extension", render: a => `${a.extension || "N/A"}` },
  { key: "call", label: "Call", render: a => renderCallButton(a) },
  { key: "dnd", label: "DND", render: a => renderDndControl(a) },
  { key: "status", label: "State", render: a => {
    const status = a.status === "Unknown" ? "Outbound" : (a.status || "Outbound");
    return `<span class="pill ${cssStatus(status)}">${status}</span>`;
  } },
  { key: "duration", label: "Time in State", render: a => `<span class="duration" data-duration-person-id="${escapeAttr(a.person_id || "")}">${formatStateDuration(a)}</span>` },
  { key: "display_name", label: "Display Name", render: a => `${a.display_name || "N/A"}` },
  { key: "webex_state", label: "Webex State", render: a => `${a.webex_state || "N/A"}` },
  { key: "event_type", label: "Event Type", render: a => `${a.event_type || "N/A"}` },
  { key: "remote_name", label: "Remote Party", render: a => `${a.remote_name || "N/A"}` },
  { key: "remote_number", label: "Remote Number", render: a => `${a.remote_number || "N/A"}` },
  { key: "transfer", label: "Transfer", render: a => renderTransferButton(a) },
  { key: "reset_status", label: "Reset", render: a => renderResetStatusButton(a) }
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


let currentTransferExtension = "";

function showTransferStatus(message, type = "info") {
  const el = document.getElementById("transferStatus");
  el.textContent = message;
  el.className = `transfer-status ${type}`;
}

function clearTransferStatus() {
  const el = document.getElementById("transferStatus");
  el.textContent = "";
  el.className = "transfer-status";
}



function escapeAttr(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function isDndDropdownActive() {
  const active = document.activeElement;
  return Boolean(active && active.classList && active.classList.contains("dnd-select"));
}

function shouldHoldTableRender() {
  return dndChangeInProgress || isDndDropdownActive();
}

function updateDurationCells() {
  document.querySelectorAll(".duration[data-duration-person-id]").forEach(cell => {
    const personId = cell.getAttribute("data-duration-person-id");
    const agent = agents.find(a => String(a.person_id || "") === personId);
    if (agent) cell.textContent = formatStateDuration(agent);
  });
}

function dndValue(agent) {
  if (agent.dnd_enabled === true || agent.dnd_enabled === 1 || agent.dnd_enabled === "1" || agent.dnd_enabled === "true") return true;
  if (agent.dnd_enabled === false || agent.dnd_enabled === 0 || agent.dnd_enabled === "0" || agent.dnd_enabled === "false") return false;
  return null;
}

function renderDndControl(agent) {
  const dnd = dndValue(agent);
  const isOn = dnd === true;
  const isOff = dnd === false;
  const personId = escapeAttr(agent.person_id || "");
  const userLabel = escapeAttr(agent.email || agent.display_name || "this user");
  const disabled = !agent.person_id || !agent.authenticated || dndChangeInProgress;
  const statusClass = isOn ? "on" : (isOff ? "off" : "unknown");
  const statusText = isOn ? "DND On" : (isOff ? "DND Off" : "DND Unknown");
  const title = disabled ? "DND unavailable until this user authenticates or another DND update is running" : "Change Do Not Disturb";

  return `<div class="dnd-actions" title="${title}">
    <span class="dnd-status ${statusClass}" data-dnd-status-for="${personId}">${statusText}</span>
    <button class="dnd-btn on" ${disabled || isOn ? "disabled" : ""} data-person-id="${personId}" data-user-label="${userLabel}" data-enabled="true">On</button>
    <button class="dnd-btn off" ${disabled || isOff ? "disabled" : ""} data-person-id="${personId}" data-user-label="${userLabel}" data-enabled="false">Off</button>
  </div>`;
}

async function setDnd(personId, enabled, userLabel, controlEl = null) {
  if (!personId || dndChangeInProgress) return;

  // Native confirm()/alert() dialogs can be unreliable or blocked inside Webex embedded iframes.
  // DND buttons now send the request immediately and show the result in the in-page status banner.
  dndChangeInProgress = true;
  const container = controlEl ? controlEl.closest(".dnd-actions") : null;
  const buttons = container ? Array.from(container.querySelectorAll(".dnd-btn")) : [];
  buttons.forEach(btn => btn.disabled = true);
  showTransferStatus(`${enabled ? "Turning on" : "Turning off"} DND for ${userLabel}...`, "info");

  try {
    const res = await fetch(`/api/agents/${encodeURIComponent(personId)}/dnd`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ enabled })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.success) {
      showTransferStatus(data.detail || data.message || "DND update failed. Check Render logs and Webex permissions.", "error");
      return;
    }

    const localAgent = agents.find(a => String(a.person_id || "") === String(personId));
    if (localAgent) localAgent.dnd_enabled = enabled;
    showTransferStatus(`DND ${enabled ? "enabled" : "disabled"} for ${userLabel}.`, "success");
    renderTable(true);
  } catch (err) {
    console.error(err);
    showTransferStatus("DND request failed before it reached the server. Check browser console and Render logs.", "error");
  } finally {
    dndChangeInProgress = false;
    await loadAgents();
  }
}

async function refreshDnd() {
  showTransferStatus("Refreshing DND settings from Webex...", "info");
  try {
    const res = await fetch("/api/refresh-dnd", { method: "POST", credentials: "same-origin" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      showTransferStatus(data.detail || "DND refresh failed. Check Render logs and Webex admin token permissions.", "error");
      return;
    }
    showTransferStatus(`DND refresh complete. Updated ${data.updated || 0} user(s).`, "success");
    await loadAgents();
  } catch (err) {
    console.error(err);
    showTransferStatus("DND refresh failed before it reached the server.", "error");
  }
}


function renderCallButton(agent) {
  const hasExtension = Boolean(agent.extension);
  const disabled = !hasExtension;
  const title = hasExtension ? "Call this user from your signed-in Webex account" : "Call unavailable: no extension found";
  const extension = JSON.stringify(agent.extension || "");
  const label = JSON.stringify(agent.email || agent.display_name || "Unknown User");
  return `<button class="call-btn" ${disabled ? "disabled" : ""} title="${title}" onclick='callUser(${extension}, ${label})'>Call</button>`;
}

async function callUser(extension, userLabel) {
  if (!extension) return;

  clearTransferStatus();
  showTransferStatus(`Calling ${userLabel} at extension ${extension}...`, "info");

  try {
    const res = await fetch("/api/call-user", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ destination: extension })
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok || !data.success) {
      const message = data.detail || data.message || "Call failed. Make sure you are signed in through /oauth/start and Webex allows call control for your user.";
      showTransferStatus(message, "error");
      return;
    }

    showTransferStatus(`Call request sent to Webex for ${userLabel} (${extension}).`, "success");
    await loadAgents();
    await loadEvents();
  } catch (err) {
    console.error(err);
    showTransferStatus("Call request failed before it reached the server. Check browser console and Render logs.", "error");
  }
}

function renderResetStatusButton(agent) {
  const disabled = !agent.person_id || !agent.authenticated;
  const title = disabled ? "Reset unavailable until this user authenticates" : "Reset this row back to Not On Call";
  const personId = JSON.stringify(agent.person_id || "");
  const label = JSON.stringify(agent.email || agent.display_name || "this user");
  return `<button class="reset-status-btn" ${disabled ? "disabled" : ""} title="${title}" onclick='resetAgentStatus(${personId}, ${label})'>Reset</button>`;
}

async function resetAgentStatus(personId, userLabel) {
  if (!personId) return;
  if (!confirm(`Reset ${userLabel} to Not On Call in the dashboard? This only updates the local dashboard row.`)) return;

  showTransferStatus(`Resetting ${userLabel} to Not On Call...`, "info");

  try {
    const res = await fetch(`/api/agents/${encodeURIComponent(personId)}/reset-status`, {
      method: "POST",
      credentials: "same-origin"
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      showTransferStatus(data.detail || data.message || "Reset status failed. Check Render logs.", "error");
      return;
    }
    showTransferStatus(`${userLabel} was reset to Not On Call.`, "success");
    await loadAgents();
  } catch (err) {
    console.error(err);
    showTransferStatus("Reset status request failed before it reached the server.", "error");
  }
}

function renderTransferButton(agent) {
  const hasExtension = Boolean(agent.extension);
  const isAvailable = agent.status === "Not On Call";
  const disabled = !(hasExtension && isAvailable);

  let title = "Transfer your active Webex call to this user";
  if (!hasExtension) {
    title = "Transfer unavailable: no extension found";
  } else if (!isAvailable) {
    title = "Transfer unavailable: user is not available";
  }

  const extension = JSON.stringify(agent.extension || "");
  const label = JSON.stringify(agent.email || agent.display_name || "Unknown User");

  return `<button class="transfer-btn" ${disabled ? "disabled" : ""} title="${title}" onclick='transferMyActiveCall(${extension}, ${label})'>Transfer</button>`;
}

async function transferMyActiveCall(extension, userLabel) {
  if (!extension) return;

  clearTransferStatus();
  showTransferStatus(`Transferring your active call to ${userLabel} at extension ${extension}...`, "info");

  try {
    const res = await fetch("/api/transfer-my-call", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ destination: extension })
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok || !data.success) {
      const message = data.detail || data.message || "Transfer failed. Make sure you are signed in through /oauth/start and currently have one active Webex Calling call.";
      showTransferStatus(message, "error");
      return;
    }

    showTransferStatus(`Transfer request sent to Webex for ${userLabel} (${extension}).`, "success");
    await loadAgents();
    await loadEvents();
  } catch (err) {
    console.error(err);
    showTransferStatus("Transfer request failed before it reached the server. Check browser console and Render logs.", "error");
  }
}

function openTransferModal(extension, userLabel) {
  if (!extension) return;

  currentTransferExtension = extension;

  document.getElementById("transferModalTitle").textContent = `Transfer to ${userLabel}`;
  document.getElementById("transferModalText").textContent =
    `Fallback mode: copy this extension and use the Webex transfer control if the direct API transfer is unavailable.`;

  document.getElementById("transferExtension").textContent = extension;
  document.getElementById("transferDialLink").href = `tel:${encodeURIComponent(extension)}`;
  document.getElementById("transferModal").style.display = "flex";
}

function closeTransferModal() {
  document.getElementById("transferModal").style.display = "none";
}

async function copyTransferExtension() {
  try {
    await navigator.clipboard.writeText(currentTransferExtension);
    alert(`Copied extension: ${currentTransferExtension}`);
  } catch {
    alert(`Extension: ${currentTransferExtension}`);
  }
}


function cssStatus(status) {
  if (status === "On Call") return "OnCall";
  if (status === "Not On Call") return "NotOnCall";
  if (status === "Ringing") return "Ringing";
  if (status === "Outbound") return "Outbound";
  if (status === "Needs Refresh") return "NeedsRefresh";
  return "Unknown";
}

function formatStateDuration(agent) {
  if (agent.is_stale || agent.status === "Needs Refresh") {
    const original = agent.original_status ? ` (${agent.original_status})` : "";
    return `Needs Refresh${original}`;
  }
  return durationSince(agent.state_started_at || agent.updated_at);
}

function durationSince(value) {
  if (!value) return "N/A";
  const start = new Date(value).getTime();
  if (Number.isNaN(start)) return "N/A";
  const total = Math.max(0, Math.floor((Date.now() - start) / 1000));
  if (total > 86400) return "Needs Refresh";
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

function formatEventTime(value) {
  if (!value) return "Unknown time";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
}

function eventStatusFromRow(event) {
  const state = (event.webex_state || "").toLowerCase();
  const type = (event.event_type || "").toLowerCase();
  if (["alerting", "ringing"].includes(state) || ["received", "offered"].includes(type)) return "Ringing";
  if (["connected", "active", "held", "remoteheld", "bridged", "consulting", "conference"].includes(state) || ["answered", "connected"].includes(type)) return "On Call";
  if (["deleted", "ended", "released", "disconnected"].includes(type)) return "Not On Call";
  return "Outbound";
}

async function loadEvents() {
  try {
    const res = await fetch("/api/events?limit=12", { cache: "no-store" });
    const data = await res.json();
    recentEvents = data.events || [];
    renderEvents();
  } catch (err) {
    console.error(err);
    document.getElementById("activityList").innerHTML = `<div class="empty">Could not load recent events.</div>`;
  }
}

function renderEvents() {
  const list = document.getElementById("activityList");
  if (!recentEvents.length) {
    list.innerHTML = `<div class="empty">No webhook events received yet.</div>`;
    return;
  }

  list.innerHTML = recentEvents.map(event => {
    const user = event.authenticated ? (event.agent_email || event.agent_display_name || "Unknown User") : "Unauthenticated Webex User";
    const org = event.org_name || event.org_id || "Unknown Org";
    const status = eventStatusFromRow(event);
    const meta = [event.webex_state, event.event_type].filter(Boolean).join(" / ") || "No state details";
    return `
      <div class="activity-item">
        <div class="activity-time">${formatEventTime(event.created_at)}</div>
        <div class="activity-user" title="${user}">${user}</div>
        <div><span class="pill ${cssStatus(status)}">${status}</span></div>
        <div class="activity-meta" title="${org} • ${meta}">${org} • ${meta}</div>
      </div>
    `;
  }).join("");
}

async function loadAgents() {
  try {
    const res = await fetch("/api/agents", { cache: "no-store" });
    const data = await res.json();
    agents = data.agents || [];
    populateOrgFilter();
    if (!shouldHoldTableRender()) {
      renderTable();
    } else {
      updateDurationCells();
    }
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
  document.getElementById("staleCount").textContent = agents.filter(a => a.status === "Needs Refresh" || a.is_stale).length;
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
    const status = a.status === "Unknown" ? "Outbound" : (a.status || "Outbound");
    const matchesOrg = orgFilter === "All" || orgName === orgFilter;
    const matchesState = stateFilter === "All" || status === stateFilter;
    const blob = JSON.stringify({ ...a, status }).toLowerCase();
    return matchesOrg && matchesState && blob.includes(search);
  });

  const body = document.getElementById("agentBody");
  if (!filtered.length) {
    body.innerHTML = `<tr><td colspan="${columns.length}" class="empty">No users match the current filters.</td></tr>`;
    return;
  }

  const grouped = filtered.reduce((acc, agent) => {
    const orgName = agent.org_name || agent.org_id || "Unknown Org";
    if (!acc[orgName]) acc[orgName] = [];
    acc[orgName].push(agent);
    return acc;
  }, {});

  body.innerHTML = Object.entries(grouped).map(([orgName, orgAgents]) => `
    <tr class="org-row"><td colspan="${columns.length}">${orgName}<span class="org-count">${orgAgents.length} user${orgAgents.length === 1 ? "" : "s"}</span></td></tr>
    ${orgAgents.map(a => `<tr>${columns.map(c => `<td>${c.render(a)}</td>`).join("")}</tr>`).join("")}
  `).join("");
}

async function removeAgent(personId) {
  if (!confirm("Remove this user from the dashboard? This only removes the local row. To delete the Webex webhook, the user should use /oauth/remove/start.")) return;
  await fetch(`/api/agents/${encodeURIComponent(personId)}/remove`, { method: "POST" });
  await loadAgents();
}


function attachDndButtonHandler() {
  document.addEventListener("click", event => {
    const button = event.target.closest && event.target.closest(".dnd-btn");
    if (!button || button.disabled) return;
    const personId = button.getAttribute("data-person-id") || "";
    const userLabel = button.getAttribute("data-user-label") || "this user";
    const enabled = button.getAttribute("data-enabled") === "true";
    setDnd(personId, enabled, userLabel, button);
  });
}

attachDndButtonHandler();
loadAgents();
loadEvents();

// Keep the original fast refresh rate, but only update timer text every second.
// This prevents controls like the DND buttons from being destroyed while a user is selecting an option.
setInterval(loadAgents, 3000);
setInterval(loadEvents, 5000);
setInterval(updateDurationCells, 1000);
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
def api_agents(show_unauthenticated: bool = False):
    with db() as conn:
        rows = conn.execute("SELECT * FROM agents").fetchall()

    hidden_unauthenticated = 0
    agents = []
    for row in rows:
        authenticated = is_authenticated_agent_row(row)
        if not authenticated and not show_unauthenticated:
            hidden_unauthenticated += 1
            continue

        agent = dict(row)
        agent["authenticated"] = authenticated
        agent["dnd_enabled"] = db_to_bool(agent.get("dnd_enabled"))
        agent["dnd_ring_reminder"] = db_to_bool(agent.get("dnd_ring_reminder"))

        if not authenticated:
            agent["email"] = None
            agent["display_name"] = "Unauthenticated Webex User"

        agent = apply_dashboard_status_rules(agent)
        agents.append(agent)

    priority = {"Ringing": 0, "On Call": 1, "Outbound": 2, "Needs Refresh": 3, "Unknown": 4, "Not On Call": 5}
    agents.sort(key=lambda a: (
        a.get("org_name") or "",
        priority.get(a.get("status"), 9),
        a.get("email") or a.get("display_name") or "",
    ))

    return {
        "count": len(agents),
        "hidden_unauthenticated": hidden_unauthenticated,
        "stale_status_after_seconds": STALE_STATUS_AFTER_SECONDS,
        "agents": agents,
    }


@app.get("/api/events")
def api_events(limit: int = 25):
    safe_limit = max(1, min(limit, 100))

    with db() as conn:
        rows = conn.execute("""
            SELECT
                e.id, e.person_id, e.org_id, e.org_name, e.event_type, e.webex_state,
                e.call_id, e.call_session_id, e.created_at,
                CASE
                    WHEN a.access_token IS NOT NULL OR a.refresh_token IS NOT NULL OR (a.email IS NOT NULL AND a.email != a.person_id AND instr(a.email, '@') > 0)
                    THEN a.email
                    ELSE NULL
                END AS agent_email,
                CASE
                    WHEN a.access_token IS NOT NULL OR a.refresh_token IS NOT NULL OR (a.email IS NOT NULL AND a.email != a.person_id AND instr(a.email, '@') > 0)
                    THEN a.display_name
                    ELSE 'Unauthenticated Webex User'
                END AS agent_display_name,
                CASE
                    WHEN a.access_token IS NOT NULL OR a.refresh_token IS NOT NULL OR (a.email IS NOT NULL AND a.email != a.person_id AND instr(a.email, '@') > 0)
                    THEN a.extension
                    ELSE NULL
                END AS agent_extension,
                CASE
                    WHEN a.access_token IS NOT NULL OR a.refresh_token IS NOT NULL OR (a.email IS NOT NULL AND a.email != a.person_id AND instr(a.email, '@') > 0)
                    THEN 1
                    ELSE 0
                END AS authenticated
            FROM events e
            LEFT JOIN agents a ON a.person_id = e.person_id
            ORDER BY e.created_at DESC, e.id DESC
            LIMIT ?
        """, (safe_limit,)).fetchall()

    return {"count": len(rows), "events": [dict(row) for row in rows]}



@app.post("/api/agents/{person_id}/dnd")
async def api_set_agent_dnd(person_id: str, request: Request):
    payload = await request.json()
    enabled = bool(payload.get("enabled"))
    ring_reminder = payload.get("ringReminderEnabled")
    if ring_reminder is not None:
        ring_reminder = bool(ring_reminder)

    with db() as conn:
        row = conn.execute("SELECT person_id, email, display_name, org_id FROM agents WHERE person_id = ?", (person_id,)).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="User was not found in the attendant console database.")

    result = set_person_dnd_settings(person_id, row["org_id"], enabled, ring_reminder)
    return {
        "success": True,
        "person_id": person_id,
        "display_name": row["display_name"],
        "email": row["email"],
        **result,
    }


@app.post("/api/refresh-dnd")
def api_refresh_dnd():
    updated = 0
    failed = []

    with db() as conn:
        rows = conn.execute("SELECT person_id, org_id FROM agents").fetchall()

    for row in rows:
        # Skip raw webhook-only placeholders where possible.
        with db() as conn:
            agent_row = conn.execute("SELECT * FROM agents WHERE person_id = ?", (row["person_id"],)).fetchone()
        if not is_authenticated_agent_row(agent_row):
            continue

        result = refresh_dnd_for_agent(row["person_id"], row["org_id"])
        if result:
            updated += 1
        else:
            failed.append(row["person_id"])

    return {"message": "DND refresh complete", "updated": updated, "failed": failed[:25], "failed_count": len(failed)}


@app.post("/api/call-user")
async def api_call_user(request: Request):
    session_id = request.cookies.get("attendant_session")
    person_id = person_id_from_session(session_id)

    if not person_id:
        raise HTTPException(status_code=401, detail="You are not signed in for call control. Open /oauth/start as the Webex user who will press Call, then return to the dashboard.")

    payload = await request.json()
    destination = str(payload.get("destination") or "").strip()
    if not destination:
        raise HTTPException(status_code=400, detail="Missing call destination extension or number.")

    access_token = get_user_token_for_call_control(person_id)
    result = dial_from_my_webex(access_token, destination)
    return {
        "success": True,
        "destination": destination,
        "call_endpoint": result.get("endpoint"),
        "webex_status_code": result.get("status_code"),
        "webex_response": result.get("response"),
    }


@app.post("/api/transfer-my-call")
async def api_transfer_my_call(request: Request):
    session_id = request.cookies.get("attendant_session")
    person_id = person_id_from_session(session_id)

    if not person_id:
        raise HTTPException(status_code=401, detail="You are not signed in for call control. Open /oauth/start as the Webex user who will press Transfer, then return to the dashboard.")

    payload = await request.json()
    destination = str(payload.get("destination") or "").strip()
    if not destination:
        raise HTTPException(status_code=400, detail="Missing transfer destination extension or number.")

    access_token = get_user_token_for_call_control(person_id)
    call_list = list_my_active_calls(access_token)
    calls = call_list.get("items") or []

    if not calls:
        raise HTTPException(status_code=409, detail="No active Webex calls were found for the signed-in user. Start or answer a call, then try Transfer again.")

    selected_call = pick_transferable_call(calls)
    if not selected_call:
        raise HTTPException(status_code=409, detail=f"Found {len(calls)} active calls, but could not determine which one to transfer.")

    call_id = extract_call_id(selected_call)
    if not call_id:
        raise HTTPException(status_code=502, detail=f"Webex returned an active call but no call ID was available: {selected_call}")

    result = transfer_my_call(access_token, call_id, destination)
    return {
        "success": True,
        "destination": destination,
        "call_id": call_id,
        "call_state": selected_call.get("state") or selected_call.get("status"),
        "list_endpoint": call_list.get("endpoint"),
        "transfer_endpoint": result.get("endpoint"),
        "webex_status_code": result.get("status_code"),
    }


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


@app.post("/api/agents/{person_id}/reset-status")
def api_reset_agent_status(person_id: str):
    ts = now_iso()
    with db() as conn:
        row = conn.execute("SELECT person_id FROM agents WHERE person_id = ?", (person_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User was not found in the dashboard database.")

        conn.execute(
            """
            UPDATE agents
            SET status = 'Not On Call',
                webex_state = NULL,
                event_type = NULL,
                call_id = NULL,
                call_session_id = NULL,
                remote_name = NULL,
                remote_number = NULL,
                remote_call_type = NULL,
                state_started_at = ?,
                updated_at = ?
            WHERE person_id = ?
            """,
            (ts, ts, person_id),
        )

        updated = conn.execute("SELECT * FROM agents WHERE person_id = ?", (person_id,)).fetchone()

    return {"message": "agent status reset", "agent": dict(updated) if updated else None}


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


@app.post("/api/events/clear")
def api_clear_events():
    """Clear all stored webhook activity without removing dashboard users."""
    result = clear_webhook_event_history(vacuum=True)
    return {
        "message": "webhook event history cleared",
        **result,
    }


@app.post("/api/maintenance/cleanup")
def api_maintenance_cleanup():
    """Manually trim webhook history and compact SQLite after a busy period."""
    with db() as conn:
        before = conn.execute("SELECT COUNT(*) AS count FROM events").fetchone()["count"]
        cleanup_event_history(conn)
        after = conn.execute("SELECT COUNT(*) AS count FROM events").fetchone()["count"]

    # VACUUM must run outside the transaction context.
    with db() as conn:
        conn.execute("VACUUM")

    return {
        "message": "maintenance cleanup complete",
        "events_before": before,
        "events_after": after,
        "max_event_rows": MAX_EVENT_ROWS,
    }


@app.get("/oauth/start")
def oauth_start():
    if not WEBEX_CLIENT_ID:
        return HTMLResponse("Missing WEBEX_CLIENT_ID in Render environment variables.", status_code=200)

    if not WEBEX_REDIRECT_URI:
        return HTMLResponse("Missing WEBEX_REDIRECT_URI in Render environment variables.", status_code=200)

    auth_url = build_webex_authorize_url(WEBEX_REDIRECT_URI, "webex-calling-attendant-console")
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

    token_json = token_response.json()
    access_token = token_json.get("access_token")
    me = get_me(access_token)
    webhook = create_call_status_webhook(access_token)

    session_id = None
    if me:
        upsert_agent_from_oauth(me, webhook, access_token, token_json)
        if me.get("id"):
            session_id = create_user_session(me.get("id"))

    response = RedirectResponse("/attendantconsole?connected=1", status_code=302)
    if session_id:
        response.set_cookie(
            key="attendant_session",
            value=session_id,
            httponly=True,
            secure=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 90,
        )
    return response


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
