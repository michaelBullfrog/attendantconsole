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

app = FastAPI(title="Webex Attendant Console")

WEBEX_CLIENT_ID = os.getenv("WEBEX_CLIENT_ID")
WEBEX_CLIENT_SECRET = os.getenv("WEBEX_CLIENT_SECRET")
WEBEX_REDIRECT_URI = os.getenv("WEBEX_REDIRECT_URI")
WEBEX_WEBHOOK_TARGET_URL = os.getenv("WEBEX_WEBHOOK_TARGET_URL")
WEBEX_ADMIN_TOKEN = os.getenv("WEBEX_ADMIN_TOKEN")

REVIO_BASE_URL = os.getenv("REVIO_BASE_URL", "").rstrip("/")
REVIO_API_KEY = os.getenv("REVIO_API_KEY")
REVIO_DEFAULT_TICKET_TYPE_ID = int(os.getenv("REVIO_DEFAULT_TICKET_TYPE_ID", "1"))
REVIO_DEFAULT_STATUS_ID = int(os.getenv("REVIO_DEFAULT_STATUS_ID", "1"))
REVIO_DEFAULT_PRIORITY_ID = int(os.getenv("REVIO_DEFAULT_PRIORITY_ID", "1"))
REVIO_DEFAULT_ACCOUNT_ID = os.getenv("REVIO_DEFAULT_ACCOUNT_ID")

INTERNAL_DOMAINS = [
    domain.strip().lower()
    for domain in os.getenv("INTERNAL_DOMAINS", "bullfrog.net,bullfroggroup.net").split(",")
    if domain.strip()
]
MIN_MEETING_MINUTES = int(os.getenv("MIN_MEETING_MINUTES", "10"))
AUTO_SEND_MEETINGS_TO_REV = os.getenv("AUTO_SEND_MEETINGS_TO_REV", "false").lower() == "true"


# Optional manual fallback:
# WEBEX_ORG_NAME_MAP={"orgIdHere":"Friendly Org Name"}
WEBEX_ORG_NAME_MAP_RAW = os.getenv("WEBEX_ORG_NAME_MAP", "{}")
try:
    WEBEX_ORG_NAME_MAP = json.loads(WEBEX_ORG_NAME_MAP_RAW)
except Exception:
    WEBEX_ORG_NAME_MAP = {}

# Keep user OAuth scopes focused on what the user needs to authorize.
# Use WEBEX_ADMIN_TOKEN for org displayName and extension enrichment.
SCOPES = "spark:calls_read spark:webhooks_write spark:webhooks_read spark:people_read spark-admin:organizations_read spark-admin:people_read meeting:schedules_read meeting:participants_read meeting:transcripts_read meeting:summaries_read"

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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_tokens (
                person_id TEXT PRIMARY KEY,
                email TEXT,
                display_name TEXT,
                access_token TEXT NOT NULL,
                refresh_token TEXT,
                expires_at TEXT,
                scope TEXT,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS meeting_rev_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                meeting_id TEXT NOT NULL,
                person_id TEXT,
                user_email TEXT,
                title TEXT,
                host_email TEXT,
                start_time TEXT,
                end_time TEXT,
                duration_minutes INTEGER,
                external_count INTEGER DEFAULT 0,
                participant_count INTEGER DEFAULT 0,
                rev_ticket_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                source TEXT,
                created_at TEXT NOT NULL,
                sent_at TEXT,
                UNIQUE(meeting_id, user_email)
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



def store_user_token(me: Dict[str, Any], token_payload: Dict[str, Any]):
    person_id = me.get("id")
    if not person_id:
        return

    emails = me.get("emails") or []
    email = emails[0] if emails else None
    display_name = me.get("displayName") or email or person_id

    access_token = token_payload.get("access_token")
    refresh_token = token_payload.get("refresh_token")
    expires_in = token_payload.get("expires_in")
    scope = token_payload.get("scope")

    expires_at = None
    if expires_in:
        try:
            expires_at_dt = datetime.now(timezone.utc).timestamp() + int(expires_in) - 300
            expires_at = datetime.fromtimestamp(expires_at_dt, timezone.utc).isoformat()
        except Exception:
            expires_at = None

    if not access_token:
        return

    with db() as conn:
        conn.execute("""
            INSERT INTO user_tokens (
                person_id, email, display_name, access_token, refresh_token,
                expires_at, scope, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                email = excluded.email,
                display_name = excluded.display_name,
                access_token = excluded.access_token,
                refresh_token = COALESCE(excluded.refresh_token, user_tokens.refresh_token),
                expires_at = excluded.expires_at,
                scope = excluded.scope,
                updated_at = excluded.updated_at
        """, (
            person_id, email, display_name, access_token, refresh_token,
            expires_at, scope, now_iso(),
        ))


def get_user_token_by_person_or_email(person_id: Optional[str] = None, email: Optional[str] = None) -> Optional[Dict[str, Any]]:
    with db() as conn:
        if person_id:
            row = conn.execute(
                "SELECT * FROM user_tokens WHERE person_id = ?",
                (person_id,),
            ).fetchone()
        elif email:
            row = conn.execute(
                "SELECT * FROM user_tokens WHERE lower(email) = lower(?)",
                (email,),
            ).fetchone()
        else:
            row = None

    return dict(row) if row else None


def refresh_user_access_token(token_row: Dict[str, Any]) -> Dict[str, Any]:
    refresh_token = token_row.get("refresh_token")
    if not refresh_token:
        return token_row

    response = requests.post(
        "https://webexapis.com/v1/access_token",
        data={
            "grant_type": "refresh_token",
            "client_id": WEBEX_CLIENT_ID,
            "client_secret": WEBEX_CLIENT_SECRET,
            "refresh_token": refresh_token,
        },
        timeout=20,
    )

    if response.status_code >= 400:
        print(f"Unable to refresh token for {token_row.get('email')}: {response.status_code} {response.text}")
        return token_row

    payload = response.json()
    access_token = payload.get("access_token")
    new_refresh_token = payload.get("refresh_token") or refresh_token
    expires_in = payload.get("expires_in")
    scope = payload.get("scope") or token_row.get("scope")

    expires_at = None
    if expires_in:
        try:
            expires_at_dt = datetime.now(timezone.utc).timestamp() + int(expires_in) - 300
            expires_at = datetime.fromtimestamp(expires_at_dt, timezone.utc).isoformat()
        except Exception:
            expires_at = None

    with db() as conn:
        conn.execute("""
            UPDATE user_tokens
            SET access_token = ?, refresh_token = ?, expires_at = ?, scope = ?, updated_at = ?
            WHERE person_id = ?
        """, (
            access_token,
            new_refresh_token,
            expires_at,
            scope,
            now_iso(),
            token_row.get("person_id"),
        ))

    token_row["access_token"] = access_token
    token_row["refresh_token"] = new_refresh_token
    token_row["expires_at"] = expires_at
    token_row["scope"] = scope
    return token_row


def get_valid_user_token(person_id: Optional[str] = None, email: Optional[str] = None) -> Dict[str, Any]:
    token_row = get_user_token_by_person_or_email(person_id, email)
    if not token_row:
        raise HTTPException(
            status_code=404,
            detail="No stored Webex token found for that user. Have the user sign in again through /oauth/start."
        )

    expires_at = token_row.get("expires_at")
    if expires_at:
        expires_dt = parse_webex_time(expires_at)
        if expires_dt and expires_dt <= datetime.now(timezone.utc):
            token_row = refresh_user_access_token(token_row)

    return token_row


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



def revio_headers() -> Dict[str, str]:
    if not REVIO_API_KEY:
        raise HTTPException(status_code=500, detail="Missing REVIO_API_KEY")

    return {
        "Authorization": f"Bearer {REVIO_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def is_internal_email(email: Optional[str]) -> bool:
    if not email or "@" not in email:
        return False
    domain = email.split("@", 1)[1].lower()
    return domain in INTERNAL_DOMAINS


def parse_webex_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def get_meeting_id_from_webhook(payload: Dict[str, Any]) -> Optional[str]:
    data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
    return (
        data.get("meetingId")
        or data.get("meeting_id")
        or data.get("id")
        or data.get("meetingUUID")
        or payload.get("meetingId")
        or payload.get("id")
    )


def webex_user_headers(token_row: Dict[str, Any]) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token_row.get('access_token')}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def webex_user_get(token_row: Dict[str, Any], url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    response = requests.get(url, headers=webex_user_headers(token_row), params=params, timeout=30)

    if response.status_code == 401:
        token_row = refresh_user_access_token(token_row)
        response = requests.get(url, headers=webex_user_headers(token_row), params=params, timeout=30)

    return response


def get_webex_meeting_details_for_user(token_row: Dict[str, Any], meeting_id: str) -> Dict[str, Any]:
    response = webex_user_get(
        token_row,
        f"https://webexapis.com/v1/meetings/{meeting_id}",
    )

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


def get_webex_meeting_participants_for_user(token_row: Dict[str, Any], meeting_id: str) -> list:
    response = webex_user_get(
        token_row,
        "https://webexapis.com/v1/meetingParticipants",
        params={"meetingId": meeting_id},
    )

    if response.status_code >= 400:
        print(f"Meeting participants lookup failed for {token_row.get('email')}: {response.status_code} {response.text}")
        return []

    return response.json().get("items", [])


def normalize_participant(participant: Dict[str, Any]) -> Dict[str, Optional[str]]:
    email = (
        participant.get("email")
        or participant.get("participantEmail")
        or participant.get("personEmail")
        or participant.get("hostEmail")
    )

    name = (
        participant.get("displayName")
        or participant.get("name")
        or participant.get("participantName")
        or participant.get("personDisplayName")
        or email
        or "Unknown Participant"
    )

    return {
        "name": name,
        "email": email,
        "joined": participant.get("joinedTime") or participant.get("joinTime"),
        "left": participant.get("leftTime") or participant.get("leaveTime"),
    }


def summarize_meeting_for_rev(meeting: Dict[str, Any], participants: list, meeting_id: str, token_row: Dict[str, Any]) -> Dict[str, Any]:
    title = meeting.get("title") or meeting.get("topic") or "Webex Meeting"
    host_email = (meeting.get("hostEmail") or meeting.get("host") or meeting.get("hostUserEmail") or "").lower()
    user_email = (token_row.get("email") or "").lower()

    start_raw = (
        meeting.get("actualStart")
        or meeting.get("start")
        or meeting.get("startTime")
        or meeting.get("scheduledStart")
    )

    end_raw = (
        meeting.get("actualEnd")
        or meeting.get("end")
        or meeting.get("endTime")
        or meeting.get("scheduledEnd")
    )

    start_dt = parse_webex_time(start_raw)
    end_dt = parse_webex_time(end_raw)

    duration_minutes = 0
    if start_dt and end_dt:
        duration_minutes = max(0, int((end_dt - start_dt).total_seconds() // 60))
    elif meeting.get("duration"):
        try:
            duration_minutes = int(meeting.get("duration"))
        except Exception:
            duration_minutes = 0

    normalized_participants = [normalize_participant(p) for p in participants]
    participant_emails = [p["email"].lower() for p in normalized_participants if p.get("email")]

    external_participants = [
        p for p in normalized_participants
        if p.get("email") and not is_internal_email(p.get("email"))
    ]

    user_participated = (
        user_email == host_email
        or user_email in participant_emails
    )

    return {
        "meeting_id": meeting_id,
        "person_id": token_row.get("person_id"),
        "user_email": user_email,
        "title": title,
        "host_email": host_email,
        "start_time": start_raw,
        "end_time": end_raw,
        "duration_minutes": duration_minutes,
        "participants": normalized_participants,
        "participant_count": len(normalized_participants),
        "external_count": len(external_participants),
        "user_participated": user_participated,
        "has_external": len(external_participants) > 0,
    }


def should_send_meeting_to_rev(summary: Dict[str, Any]) -> tuple[bool, str]:
    if not summary.get("user_participated"):
        return False, f"{summary.get('user_email')} was not found as host or participant."

    if summary.get("duration_minutes", 0) < MIN_MEETING_MINUTES:
        return False, f"Meeting was under {MIN_MEETING_MINUTES} minutes."

    if not summary.get("has_external"):
        return False, "Meeting has no external attendees."

    return True, "Eligible"


def format_meeting_ticket_description(summary: Dict[str, Any]) -> str:
    participant_lines = []
    for p in summary.get("participants", []):
        email = p.get("email") or "no email"
        participant_lines.append(f"- {p.get('name')} <{email}>")

    participants_text = "\n".join(participant_lines) if participant_lines else "No participants returned by Webex."

    return f"""Webex meeting completed.

Sent By:
{summary.get("user_email") or "N/A"}

Meeting Title: {summary.get("title")}
Meeting ID: {summary.get("meeting_id")}
Start: {summary.get("start_time") or "N/A"}
End: {summary.get("end_time") or "N/A"}
Duration: {summary.get("duration_minutes", 0)} minutes

Host:
{summary.get("host_email") or "N/A"}

Participants:
{participants_text}

Source:
Auto-created from Webex meeting participation.
"""


def create_revio_ticket_for_meeting(summary: Dict[str, Any]) -> Dict[str, Any]:
    if not REVIO_BASE_URL:
        raise HTTPException(status_code=500, detail="Missing REVIO_BASE_URL")

    payload = {
        "subject": f"Meeting: {summary.get('title')}",
        "description": format_meeting_ticket_description(summary),
        "ticket_type_id": REVIO_DEFAULT_TICKET_TYPE_ID,
        "ticket_status_id": REVIO_DEFAULT_STATUS_ID,
        "ticket_priority_id": REVIO_DEFAULT_PRIORITY_ID,
        "include_custom_fields": False,
    }

    if REVIO_DEFAULT_ACCOUNT_ID:
        payload["account_id"] = REVIO_DEFAULT_ACCOUNT_ID

    response = requests.post(
        f"{REVIO_BASE_URL}/psac/api/v1/ticket",
        headers=revio_headers(),
        json=payload,
        timeout=30,
    )

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


def upsert_meeting_record(summary: Dict[str, Any], status: str, source: str, error: Optional[str] = None, rev_ticket_id: Optional[str] = None):
    with db() as conn:
        conn.execute("""
            INSERT INTO meeting_rev_tickets (
                meeting_id, person_id, user_email, title, host_email, start_time,
                end_time, duration_minutes, external_count, participant_count,
                rev_ticket_id, status, error, source, created_at, sent_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(meeting_id, user_email) DO UPDATE SET
                person_id = excluded.person_id,
                title = excluded.title,
                host_email = excluded.host_email,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                duration_minutes = excluded.duration_minutes,
                external_count = excluded.external_count,
                participant_count = excluded.participant_count,
                rev_ticket_id = COALESCE(excluded.rev_ticket_id, meeting_rev_tickets.rev_ticket_id),
                status = excluded.status,
                error = excluded.error,
                source = excluded.source,
                sent_at = COALESCE(excluded.sent_at, meeting_rev_tickets.sent_at)
        """, (
            summary.get("meeting_id"),
            summary.get("person_id"),
            summary.get("user_email"),
            summary.get("title"),
            summary.get("host_email"),
            summary.get("start_time"),
            summary.get("end_time"),
            summary.get("duration_minutes", 0),
            summary.get("external_count", 0),
            summary.get("participant_count", 0),
            rev_ticket_id,
            status,
            error,
            source,
            now_iso(),
            now_iso() if status == "sent" else None,
        ))


def get_meeting_records() -> list:
    with db() as conn:
        rows = conn.execute("""
            SELECT *
            FROM meeting_rev_tickets
            ORDER BY COALESCE(sent_at, created_at) DESC
            LIMIT 100
        """).fetchall()

    return [dict(row) for row in rows]


def get_connected_meeting_users() -> list:
    with db() as conn:
        rows = conn.execute("""
            SELECT person_id, email, display_name, updated_at, scope
            FROM user_tokens
            ORDER BY lower(email)
        """).fetchall()

    return [dict(row) for row in rows]


def process_meeting_to_rev(meeting_id: str, user_email: Optional[str] = None, person_id: Optional[str] = None, source: str = "manual", force: bool = False) -> Dict[str, Any]:
    token_row = get_valid_user_token(person_id=person_id, email=user_email)

    with db() as conn:
        existing = conn.execute(
            "SELECT meeting_id, status, rev_ticket_id FROM meeting_rev_tickets WHERE meeting_id = ? AND lower(user_email) = lower(?)",
            (meeting_id, token_row.get("email")),
        ).fetchone()

    if existing and existing["status"] == "sent" and not force:
        return {
            "status": "duplicate",
            "message": "This meeting was already sent to Rev.io for this user.",
            "rev_ticket_id": existing["rev_ticket_id"],
        }

    meeting = get_webex_meeting_details_for_user(token_row, meeting_id)
    participants = get_webex_meeting_participants_for_user(token_row, meeting_id)
    summary = summarize_meeting_for_rev(meeting, participants, meeting_id, token_row)

    eligible, reason = should_send_meeting_to_rev(summary)
    if not eligible and not force:
        upsert_meeting_record(summary, "skipped", source, reason)
        return {
            "status": "skipped",
            "reason": reason,
            "summary": summary,
        }

    try:
        ticket = create_revio_ticket_for_meeting(summary)
        rev_ticket_id = (
            ticket.get("id")
            or ticket.get("ticket_id")
            or ticket.get("ticket", {}).get("id")
            or ticket.get("data", {}).get("id")
        )

        upsert_meeting_record(summary, "sent", source, None, str(rev_ticket_id) if rev_ticket_id else None)

        return {
            "status": "sent",
            "rev_ticket_id": rev_ticket_id,
            "ticket_response": ticket,
            "summary": summary,
        }

    except HTTPException as exc:
        upsert_meeting_record(summary, "error", source, str(exc.detail))
        raise


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
          <p>This page connects a Webex Calling user to the status monitor.</p>
          <a class="button" href="/oauth/start">Connect Webex User</a>
          <a class="button" href="/meetings">Send Meeting to Rev</a>
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
        "has_revio_base_url": bool(REVIO_BASE_URL),
        "has_revio_api_key": bool(REVIO_API_KEY),
    }


@app.get("/supervisor", response_class=HTMLResponse)
def supervisor_dashboard():
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Webex Attendant Console</title>
  <style>
    :root {
      --bg: #eef3f8; --panel: #ffffff; --text: #101828; --muted: #667085;
      --border: #d0d5dd; --blue: #2563eb; --green-bg: #dcfce7; --green-text: #166534;
      --red-bg: #fee2e2; --red-text: #991b1b; --yellow-bg: #fef3c7; --yellow-text: #92400e;
      --gray-bg: #e5e7eb; --gray-text: #374151;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Arial, Helvetica, sans-serif; background: var(--bg); color: var(--text); }
    header {
      background: linear-gradient(135deg, #006b3a, #2f8f46);
      color: white;
      padding: 22px 30px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
    }
    .header-text h1 { margin: 0; font-size: 28px; }
    .header-text p { margin: 7px 0 0; color: #e8f7dc; font-size: 14px; }
    .header-logo-wrap {
      background: white;
      border-radius: 18px;
      padding: 8px;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.18);
      flex: 0 0 auto;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .header-logo {
      width: 72px;
      height: 72px;
      object-fit: contain;
      display: block;
    }
    main { width: 100%; max-width: none; margin: 0; padding: 24px 32px; }
    .toolbar { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; width: 100%; }
    .toolbar-left { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    input, select { border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; font-size: 14px; min-width: 240px; background: white; }
    button, a.button { border: none; border-radius: 10px; background: var(--blue); color: white; padding: 10px 14px; font-size: 14px; cursor: pointer; text-decoration: none; display: inline-block; }
    button.secondary { background: #475569; }
    button.small { padding: 7px 10px; font-size: 12px; }
    .summary { display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 14px; margin-bottom: 16px; width: 100%; }
    .summary-card { background: var(--panel); border: 1px solid var(--border); border-radius: 16px; padding: 16px; box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06); }
    .summary-card .label { color: var(--muted); font-size: 13px; }
    .summary-card .value { font-size: 30px; font-weight: 800; margin-top: 4px; }
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
    .Unknown { background: var(--gray-bg); color: var(--gray-text); }
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
      background: #dbeafe;
      color: #1d4ed8;
    }
    .transfer-btn:disabled {
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
      .header-logo { width: 56px; height: 56px; }
      main { padding: 14px; }
      .summary { grid-template-columns: repeat(2, 1fr); }
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
    <img class="header-logo" src="data:image/png;base64,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" alt="Bullfrog logo" />
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
        <option value="Unknown">Unknown</option>
      </select>
      <button onclick="loadAgents()">Refresh</button>
      <button class="secondary" onclick="refreshExtensions()">Refresh Extensions</button>
      <button class="secondary" onclick="refreshOrgs()">Refresh Orgs</button>
      <button class="secondary" onclick="resetColumnOrder()">Reset Columns</button>
      <a class="button" href="/meetings">Send Meeting to Rev</a>
    </div>
  </div>

  <section class="summary">
    <div class="summary-card"><div class="label">Total Users</div><div class="value" id="totalCount">0</div></div>
    <div class="summary-card"><div class="label">Ringing</div><div class="value" id="ringingCount">0</div></div>
    <div class="summary-card"><div class="label">On Call</div><div class="value" id="onCallCount">0</div></div>
    <div class="summary-card"><div class="label">Not On Call</div><div class="value" id="notOnCallCount">0</div></div>
  </section>

  <div class="scroll-hint">Tip: use the horizontal scrollbar at the bottom of the table to see all columns.</div>
  <div class="table-wrap">
    <table>
      <thead><tr id="tableHeader"></tr></thead>
      <tbody id="agentBody"><tr><td class="empty">Loading...</td></tr></tbody>
    </table>
  </div>

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

const DEFAULT_COLUMNS = [
  { key: "email", label: "User Email", render: a => `<div class="email" title="${a.email || "Unknown"}">${a.email || "Unknown"}</div>` },
  { key: "organization", label: "Organization", render: a => {
    const org = a.org_name || a.org_id || "Unknown Org";
    return `<span class="cell-clip" title="${org}">${org}</span>`;
  } },
  { key: "extension", label: "Extension", render: a => `${a.extension || "N/A"}` },
  { key: "status", label: "State", render: a => `<span class="pill ${cssStatus(a.status)}">${a.status || "Unknown"}</span>` },
  { key: "duration", label: "Time in State", render: a => `<span class="duration">${durationSince(a.state_started_at || a.updated_at)}</span>` },
  { key: "display_name", label: "Display Name", render: a => `${a.display_name || "N/A"}` },
  { key: "webex_state", label: "Webex State", render: a => `${a.webex_state || "N/A"}` },
  { key: "event_type", label: "Event Type", render: a => `${a.event_type || "N/A"}` },
  { key: "remote_name", label: "Remote Party", render: a => `${a.remote_name || "N/A"}` },
  { key: "remote_number", label: "Remote Number", render: a => `${a.remote_number || "N/A"}` },
  { key: "transfer", label: "Transfer", render: a => renderTransferButton(a) }
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

function renderTransferButton(agent) {
  const hasExtension = Boolean(agent.extension);
  const isAvailable = agent.status === "Not On Call";
  const disabled = !(hasExtension && isAvailable);

  let title = "Transfer to this user";
  if (!hasExtension) {
    title = "Transfer unavailable: no extension found";
  } else if (!isAvailable) {
    title = "Transfer unavailable: user is not available";
  }

  return `<button class="transfer-btn" ${disabled ? "disabled" : ""} title="${title}" onclick="openTransferModal('${agent.extension || ""}', '${agent.email || agent.display_name || "Unknown User"}')">Transfer</button>`;
}

function openTransferModal(extension, userLabel) {
  if (!extension) return;

  currentTransferExtension = extension;

  document.getElementById("transferModalTitle").textContent = `Transfer to ${userLabel}`;
  document.getElementById("transferModalText").textContent =
    `Use this extension as the transfer destination in Webex Calling. If your browser is registered to Webex, Open Dialer may start a new call; for an active call transfer, use the Webex transfer control and enter this extension.`;

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




@app.get("/meetings", response_class=HTMLResponse)
def meetings_page():
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Send Webex Meeting to Rev.io</title>
  <style>
    body { margin: 0; font-family: Arial, Helvetica, sans-serif; background: #eef3f8; color: #101828; }
    header { background: linear-gradient(135deg, #006b3a, #2f8f46); color: white; padding: 22px 30px; }
    header h1 { margin: 0; font-size: 26px; }
    header p { margin: 7px 0 0; color: #e8f7dc; font-size: 14px; }
    main { padding: 24px 32px; }
    .card { background: white; border: 1px solid #d0d5dd; border-radius: 16px; padding: 18px; box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06); margin-bottom: 18px; }
    .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    input, select { border: 1px solid #d0d5dd; border-radius: 10px; padding: 11px 12px; font-size: 14px; min-width: 320px; background: white; }
    button, a.button { border: none; border-radius: 10px; background: #2563eb; color: white; padding: 10px 14px; font-size: 14px; cursor: pointer; text-decoration: none; display: inline-block; }
    button.secondary, a.secondary { background: #475569; }
    .table-wrap { width: 100%; overflow-x: auto; }
    table { width: max-content; min-width: 1250px; border-collapse: collapse; background: white; }
    th { text-align: left; padding: 12px; background: #f8fafc; border-bottom: 1px solid #d0d5dd; font-size: 12px; text-transform: uppercase; color: #475569; white-space: nowrap; }
    td { padding: 12px; border-bottom: 1px solid #eef2f7; vertical-align: top; white-space: nowrap; }
    .pill { border-radius: 999px; padding: 5px 9px; font-size: 12px; font-weight: 800; display: inline-block; }
    .sent { background: #dcfce7; color: #166534; }
    .pending { background: #e0f2fe; color: #075985; }
    .skipped { background: #fef3c7; color: #92400e; }
    .error { background: #fee2e2; color: #991b1b; }
    .duplicate { background: #e5e7eb; color: #374151; }
    .muted { color: #667085; font-size: 13px; }
    .result { white-space: pre-wrap; font-family: Consolas, Monaco, monospace; font-size: 13px; margin-top: 12px; background: #f8fafc; border: 1px solid #d0d5dd; border-radius: 12px; padding: 12px; max-height: 360px; overflow: auto; }
  </style>
</head>
<body>
<header>
  <h1>Send Webex Meeting to Rev.io</h1>
  <p>Uses the selected signed-in user’s Webex token. A user can only send meetings their token can access and where they are host or participant.</p>
</header>

<main>
  <div class="card">
    <h2>Manual Send</h2>
    <p class="muted">Select the Webex user who signed in, paste the Webex meeting ID, then send it to Rev.io.</p>
    <div class="row">
      <select id="meetingUser"><option value="">Loading users...</option></select>
      <input id="meetingId" placeholder="Webex meeting ID" />
      <label><input type="checkbox" id="forceSend" /> Force Send</label>
      <button onclick="sendMeeting()">Send Meeting to Rev</button>
      <a class="button secondary" href="/supervisor">Back to Console</a>
    </div>
    <p class="muted">If a user is missing from the dropdown, have them sign in again through <strong>/oauth/start</strong>.</p>
    <div id="result" class="result">Ready.</div>
  </div>

  <div class="card">
    <h2>Meeting Ticket Log</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Meeting</th>
            <th>User</th>
            <th>Status</th>
            <th>Duration</th>
            <th>Participants</th>
            <th>External</th>
            <th>Rev Ticket</th>
            <th>Error/Reason</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody id="meetingRows">
          <tr><td colspan="9">Loading...</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</main>

<script>
async function loadUsers() {
  const response = await fetch("/api/meeting-users", { cache: "no-store" });
  const data = await response.json();
  const users = data.users || [];
  const select = document.getElementById("meetingUser");

  if (!users.length) {
    select.innerHTML = `<option value="">No connected users found</option>`;
    return;
  }

  select.innerHTML = users.map(u => `<option value="${u.email}">${u.email} - ${u.display_name || ""}</option>`).join("");
}

async function loadMeetings() {
  const response = await fetch("/api/meetings", { cache: "no-store" });
  const data = await response.json();
  const rows = data.meetings || [];
  const body = document.getElementById("meetingRows");

  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="9">No meetings recorded yet.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map(m => `
    <tr>
      <td><strong>${m.title || "Webex Meeting"}</strong><br><span class="muted">${m.meeting_id}</span></td>
      <td>${m.user_email || "N/A"}</td>
      <td><span class="pill ${m.status || "pending"}">${m.status || "pending"}</span></td>
      <td>${m.duration_minutes || 0} min</td>
      <td>${m.participant_count || 0}</td>
      <td>${m.external_count || 0}</td>
      <td>${m.rev_ticket_id || "N/A"}</td>
      <td>${m.error || ""}</td>
      <td><button onclick="sendExisting('${m.meeting_id}', '${m.user_email || ""}')">Send</button></td>
    </tr>
  `).join("");
}

async function sendExisting(meetingId, userEmail) {
  document.getElementById("meetingId").value = meetingId;
  if (userEmail) document.getElementById("meetingUser").value = userEmail;
  await sendMeeting();
}

async function sendMeeting() {
  const meetingId = document.getElementById("meetingId").value.trim();
  const userEmail = document.getElementById("meetingUser").value;
  const force = document.getElementById("forceSend").checked;

  if (!userEmail) {
    alert("Select a connected Webex user first.");
    return;
  }

  if (!meetingId) {
    alert("Enter a Webex meeting ID first.");
    return;
  }

  const resultEl = document.getElementById("result");
  resultEl.textContent = "Sending...";

  try {
    const response = await fetch("/api/meetings/send", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({meeting_id: meetingId, user_email: userEmail, force})
    });

    const data = await response.json();
    resultEl.textContent = JSON.stringify(data, null, 2);
    await loadMeetings();
  } catch (error) {
    resultEl.textContent = String(error);
  }
}

loadUsers();
loadMeetings();
setInterval(loadMeetings, 10000);
</script>
</body>
</html>
    """)


@app.get("/api/meeting-users")
def api_meeting_users():
    return {"users": get_connected_meeting_users()}


@app.get("/api/meetings")
def api_meetings():
    return {"meetings": get_meeting_records()}


@app.post("/api/meetings/send")
async def api_send_meeting(request: Request):
    body = await request.json()
    meeting_id = body.get("meeting_id") or body.get("meetingId")
    user_email = body.get("user_email") or body.get("userEmail")
    person_id = body.get("person_id") or body.get("personId")
    force = bool(body.get("force"))

    if not meeting_id:
        raise HTTPException(status_code=400, detail="Missing meeting_id")

    if not user_email and not person_id:
        raise HTTPException(status_code=400, detail="Missing user_email or person_id")

    return process_meeting_to_rev(
        meeting_id=meeting_id,
        user_email=user_email,
        person_id=person_id,
        source="manual",
        force=force,
    )


@app.post("/webex/meeting-ended")
async def webex_meeting_ended(request: Request):
    payload = await request.json()

    print("Received Webex meeting webhook:")
    print(json.dumps(payload, indent=2))

    meeting_id = get_meeting_id_from_webhook(payload)
    if not meeting_id:
        return {"status": "ignored", "reason": "No meeting ID found in webhook payload."}

    event = str(payload.get("event", "")).lower()
    if event and event not in {"ended", "meetingended"}:
        return {"status": "ignored", "reason": f"Unsupported meeting event: {event}", "meeting_id": meeting_id}

    data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
    person_id = data.get("personId") or data.get("hostId") or payload.get("actorId")
    user_email = data.get("hostEmail") or data.get("email")

    if AUTO_SEND_MEETINGS_TO_REV and (person_id or user_email):
        return process_meeting_to_rev(
            meeting_id=meeting_id,
            user_email=user_email,
            person_id=person_id,
            source="webhook",
            force=False,
        )

    summary = {
        "meeting_id": meeting_id,
        "person_id": person_id,
        "user_email": user_email,
        "title": "Webex Meeting",
        "host_email": user_email,
        "start_time": None,
        "end_time": None,
        "duration_minutes": 0,
        "external_count": 0,
        "participant_count": 0,
    }
    upsert_meeting_record(summary, "pending", "webhook", "Webhook received. Open /meetings and click Send to Rev.")
    return {"status": "pending", "meeting_id": meeting_id, "message": "Meeting recorded. Use /meetings to send to Rev.io."}


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
        store_user_token(me, token_response.json())

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
