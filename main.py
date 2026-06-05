import os
import json
import base64
import sqlite3
try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:
    psycopg = None
    dict_row = None
import secrets
import time
import threading
import requests
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
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
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = bool(DATABASE_URL)

# Limit stale live-call states so the dashboard does not show weekend-long timers.
# Override with STALE_STATUS_AFTER_SECONDS if you want a different limit.
STALE_STATUS_AFTER_SECONDS = int(os.getenv("STALE_STATUS_AFTER_SECONDS", "86400"))

# Keep webhook history from growing forever on small Render instances.
# MAX_EVENT_ROWS keeps only the newest rows. STORE_RAW_WEBHOOK_PAYLOADS is
# disabled by default so Recent Webex Activity can work from parsed fields
# without storing full raw webhook JSON. Set STORE_RAW_WEBHOOK_PAYLOADS=true
# only while actively troubleshooting.
MAX_EVENT_ROWS = int(os.getenv("MAX_EVENT_ROWS", "1000"))
STORE_RAW_WEBHOOK_PAYLOADS = os.getenv("STORE_RAW_WEBHOOK_PAYLOADS", "false").strip().lower() in {"1", "true", "yes", "on"}
WEBHOOK_PAYLOAD_MAX_CHARS = int(os.getenv("WEBHOOK_PAYLOAD_MAX_CHARS", "0"))

# Hidden webhook-only placeholder users can build up over time. Delete them
# automatically after this many seconds. Default is 24 hours. Set to 0 to disable.
UNAUTHENTICATED_AGENT_RETENTION_SECONDS = int(os.getenv("UNAUTHENTICATED_AGENT_RETENTION_SECONDS", "86400"))

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

# Rev.io PSA ticket settings. The Create Ticket API path is /psac/api/v1/ticket.
# Configure REVIO_PSA_BASE_URL to your Rev.io PSA API host, for example:
# https://your-tenant.reviopsa.com or the API host Rev.io provided.
REVIO_PSA_BASE_URL = os.getenv("REVIO_PSA_BASE_URL", "").rstrip("/")
REVIO_PSA_TICKET_ENDPOINT = os.getenv("REVIO_PSA_TICKET_ENDPOINT", "/psac/api/v1/ticket")
REVIO_PSA_AUTH_HEADER = os.getenv("REVIO_PSA_AUTH_HEADER", "")
REVIO_PSA_BEARER_TOKEN = os.getenv("REVIO_PSA_BEARER_TOKEN", "")
# Bot-compatible Rev.io PSA variables. Your working bot uses REVIO_PSA_API_KEY,
# REVIO_PSA_BASE_URL, REVIO_PSA_HOST, and REVIO_PSA_TICKET_* IDs.
# By default this app exchanges REVIO_PSA_API_KEY for a Rev.io JWT,
# then sends that JWT as Authorization: Bearer <token>.
REVIO_PSA_API_KEY = os.getenv("REVIO_PSA_API_KEY", "")
REVIO_PSA_HOST = os.getenv("REVIO_PSA_HOST", "")
REVIO_PSA_API_KEY_AUTH_MODE = os.getenv("REVIO_PSA_API_KEY_AUTH_MODE", "exchange").strip().lower()
REVIO_PSA_API_KEY_HEADER = os.getenv("REVIO_PSA_API_KEY_HEADER", "Ocp-Apim-Subscription-Key")
REVIO_PSA_API_KEY_SCHEME = os.getenv("REVIO_PSA_API_KEY_SCHEME", "")
REVIO_PSA_AUTH_EXCHANGE_ENDPOINT = os.getenv("REVIO_PSA_AUTH_EXCHANGE_ENDPOINT", "/api/v1/auth/api-key/exchange")
REVIO_PSA_API_KEY_EXCHANGE_BODY_FIELD = os.getenv("REVIO_PSA_API_KEY_EXCHANGE_BODY_FIELD", "apiKey")
REVIO_PSA_JWT_SAFETY_SECONDS = int(os.getenv("REVIO_PSA_JWT_SAFETY_SECONDS", "60"))
REVIO_PSA_BASIC_USERNAME = os.getenv("REVIO_PSA_BASIC_USERNAME", "")
REVIO_PSA_BASIC_PASSWORD = os.getenv("REVIO_PSA_BASIC_PASSWORD", "")
REVIO_TICKET_TYPE_ID = int(os.getenv("REVIO_TICKET_TYPE_ID", os.getenv("REVIO_PSA_TICKET_TYPE_ID", "0")))
REVIO_TICKET_STATUS_ID = int(os.getenv("REVIO_TICKET_STATUS_ID", os.getenv("REVIO_PSA_TICKET_STATUS_ID", "0")))
REVIO_TICKET_PRIORITY_ID = int(os.getenv("REVIO_TICKET_PRIORITY_ID", os.getenv("REVIO_PSA_TICKET_PRIORITY_ID", "0")))
REVIO_TICKET_CUSTOMER_ID = int(os.getenv("REVIO_TICKET_CUSTOMER_ID", "0"))
REVIO_TICKET_SEVERITY_ID = int(os.getenv("REVIO_TICKET_SEVERITY_ID", "0"))
REVIO_TICKET_USER_GROUP_TARGET = os.getenv("REVIO_TICKET_USER_GROUP_TARGET", "")
REVIO_TICKET_AUTO_CREATE_ON_CALL_END = os.getenv("REVIO_TICKET_AUTO_CREATE_ON_CALL_END", "false").strip().lower() in {"1", "true", "yes", "on"}
REVIO_INCLUDE_CUSTOM_FIELDS = os.getenv("REVIO_INCLUDE_CUSTOM_FIELDS", "false").strip().lower() in {"1", "true", "yes", "on"}

# Zoho CRM settings. This creates a CRM record from a completed call log.
# Default target is the standard Calls module, but you can point it at a custom
# module by changing ZOHO_CRM_MODULE. The API call uses Zoho CRM Insert Records:
# POST {ZOHO_CRM_BASE_URL}/crm/{ZOHO_CRM_API_VERSION}/{ZOHO_CRM_MODULE}
ZOHO_CRM_BASE_URL = os.getenv("ZOHO_CRM_BASE_URL", "https://www.zohoapis.com").rstrip("/")
ZOHO_ACCOUNTS_BASE_URL = os.getenv("ZOHO_ACCOUNTS_BASE_URL", "https://accounts.zoho.com").rstrip("/")
ZOHO_CRM_API_VERSION = os.getenv("ZOHO_CRM_API_VERSION", "v8").strip() or "v8"
ZOHO_CRM_MODULE = os.getenv("ZOHO_CRM_MODULE", "Calls").strip() or "Calls"
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID", "")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET", "")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN", "")
ZOHO_ACCESS_TOKEN = os.getenv("ZOHO_ACCESS_TOKEN", "")
ZOHO_CALL_SUBJECT_PREFIX = os.getenv("ZOHO_CALL_SUBJECT_PREFIX", "Webex Call")
ZOHO_INCLUDE_CUSTOM_FIELDS = os.getenv("ZOHO_INCLUDE_CUSTOM_FIELDS", "false").strip().lower() in {"1", "true", "yes", "on"}
ZOHO_CUSTOM_FIELD_PREFIX = os.getenv("ZOHO_CUSTOM_FIELD_PREFIX", "Webex_")
ZOHO_ACCESS_TOKEN_SAFETY_SECONDS = int(os.getenv("ZOHO_ACCESS_TOKEN_SAFETY_SECONDS", "60"))
ZOHO_TIMEZONE = os.getenv("ZOHO_TIMEZONE", "America/Detroit").strip() or "America/Detroit"
# Zoho displays DateTime values in the CRM user/account timezone. If your Zoho
# user is not in the same timezone as the Webex call log, this mode compensates
# so the wall-clock time shown in Zoho matches ZOHO_TIMEZONE.
# Options: absolute, display_wall_clock
ZOHO_TIME_SEND_MODE = os.getenv("ZOHO_TIME_SEND_MODE", "local_no_offset").strip().lower() or "local_no_offset"
ZOHO_CRM_DISPLAY_TIMEZONE = os.getenv("ZOHO_CRM_DISPLAY_TIMEZONE", "America/Los_Angeles").strip() or "America/Los_Angeles"
# Zoho's standard Call_Duration field only accepts HH:mm. Set this optional
# custom field API name if you create a Zoho Number field for exact seconds.
ZOHO_EXACT_DURATION_FIELD_API_NAME = os.getenv("ZOHO_EXACT_DURATION_FIELD_API_NAME", "").strip()

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


class PostgresDB:
    """Small compatibility wrapper so the existing app can use PostgreSQL.

    The app was originally written for sqlite3 using qmark placeholders (?).
    psycopg uses %s placeholders, so this wrapper translates the placeholders
    and returns dict-like rows that still work with row["column"] and dict(row).
    """

    def __init__(self):
        if psycopg is None:
            raise RuntimeError("DATABASE_URL is set, but psycopg is not installed. Add psycopg[binary] to requirements.txt.")
        self.conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            self.conn.close()
        return False

    def execute(self, query: str, params: tuple = ()):  # mimic sqlite connection.execute
        q = self._translate_sql(query)
        # PostgreSQL VACUUM cannot run inside this transaction wrapper. Render
        # Postgres handles vacuuming automatically, so skip manual VACUUM calls.
        if q.strip().upper() == "VACUUM":
            class NoOpCursor:
                rowcount = 0
                def fetchone(self): return None
                def fetchall(self): return []
            return NoOpCursor()
        return self.conn.execute(q, params or ())

    @staticmethod
    def _translate_sql(query: str) -> str:
        # The project queries do not contain literal ? characters, so this is safe here.
        q = query.replace("?", "%s")
        # SQLite instr(email, '@') compatibility used in /api/events.
        q = q.replace("instr(a.email, '@') > 0", "POSITION('@' IN a.email) > 0")
        return q


def db():
    if USE_POSTGRES:
        return PostgresDB()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def compact_payload(event: Dict[str, Any]) -> str:
    """Optionally store a small raw webhook JSON sample for troubleshooting.

    By default, this returns an empty string to reduce SQLite growth and memory
    pressure. Recent Webex Activity uses parsed fields from the events table, so
    the dashboard does not need the raw webhook payload for normal operation.
    """
    if not STORE_RAW_WEBHOOK_PAYLOADS or WEBHOOK_PAYLOAD_MAX_CHARS <= 0:
        return ""

    try:
        text = json.dumps(event, separators=(",", ":"))
    except Exception:
        text = str(event)

    if len(text) > WEBHOOK_PAYLOAD_MAX_CHARS:
        return text[:WEBHOOK_PAYLOAD_MAX_CHARS] + "...[truncated]"
    return text


def format_duration_seconds(seconds: int) -> str:
    seconds = max(0, int(seconds or 0))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


REVIO_JWT_CACHE: Dict[str, Any] = {"token": None, "expires_at": 0, "last_exchange_status": None, "last_exchange_error": None}
ZOHO_TOKEN_CACHE: Dict[str, Any] = {"token": None, "expires_at": 0, "last_refresh_status": None, "last_refresh_error": None}


def get_revio_auth_exchange_url() -> str:
    return f"{REVIO_PSA_BASE_URL}{REVIO_PSA_AUTH_EXCHANGE_ENDPOINT}"


def _decode_jwt_exp_seconds(token: str) -> Optional[int]:
    """Decode JWT exp without verifying it so we can cache until just before expiry."""
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_segment = parts[1]
        payload_segment += "=" * (-len(payload_segment) % 4)
        payload_bytes = base64.urlsafe_b64decode(payload_segment.encode("utf-8"))
        payload = json.loads(payload_bytes.decode("utf-8"))
        exp = payload.get("exp")
        return int(exp) if exp else None
    except Exception:
        return None


def _extract_revio_jwt(response_payload: Any) -> Optional[str]:
    """Rev.io token exchange responses can vary; search common token fields safely."""
    if isinstance(response_payload, str):
        value = response_payload.strip().strip('"')
        return value if value.count(".") >= 2 else None

    if not isinstance(response_payload, dict):
        return None

    common_keys = [
        "token", "accessToken", "access_token", "jwt", "jwtToken", "bearerToken",
        "bearer_token", "idToken", "id_token", "value", "result", "data",
    ]

    for key in common_keys:
        value = response_payload.get(key)
        if isinstance(value, str) and value.strip():
            candidate = value.strip()
            if candidate.lower().startswith("bearer "):
                candidate = candidate.split(" ", 1)[1].strip()
            if candidate.count(".") >= 2:
                return candidate
        if isinstance(value, dict):
            nested = _extract_revio_jwt(value)
            if nested:
                return nested

    # Last-resort recursive search for any JWT-looking string in a shallow payload.
    for value in response_payload.values():
        if isinstance(value, str) and value.count(".") >= 2:
            return value.strip()
        if isinstance(value, dict):
            nested = _extract_revio_jwt(value)
            if nested:
                return nested
    return None


def exchange_revio_api_key_for_jwt(force_refresh: bool = False) -> str:
    """Exchange the Rev.io PSA API key for a JWT, then cache it until near expiry."""
    if not REVIO_PSA_API_KEY:
        raise HTTPException(status_code=500, detail="REVIO_PSA_API_KEY is not configured.")

    now = int(time.time())
    cached_token = REVIO_JWT_CACHE.get("token")
    cached_expires_at = int(REVIO_JWT_CACHE.get("expires_at") or 0)
    if not force_refresh and cached_token and cached_expires_at > now + REVIO_PSA_JWT_SAFETY_SECONDS:
        return str(cached_token)

    exchange_url = get_revio_auth_exchange_url()
    exchange_headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if REVIO_PSA_HOST:
        # Rev.io expects the tenant host in this header; do not override the HTTP Host header.
        exchange_headers["X-Revio-Host"] = REVIO_PSA_HOST.strip()

    payload = {REVIO_PSA_API_KEY_EXCHANGE_BODY_FIELD: REVIO_PSA_API_KEY.strip()}

    try:
        response = requests.post(exchange_url, headers=exchange_headers, json=payload, timeout=30)
    except Exception as exc:
        REVIO_JWT_CACHE.update({"last_exchange_status": "exception", "last_exchange_error": str(exc)})
        raise HTTPException(status_code=502, detail=f"Rev.io API key exchange request failed: {exc}")

    REVIO_JWT_CACHE["last_exchange_status"] = response.status_code

    try:
        response_payload = response.json() if response.text else {}
    except Exception:
        response_payload = response.text or ""

    if response.status_code not in (200, 201, 202):
        error_body = response.text[:1500]
        REVIO_JWT_CACHE["last_exchange_error"] = f"{response.status_code}: {error_body}"
        raise HTTPException(
            status_code=502,
            detail=(
                f"Rev.io API key exchange failed. Rev.io said: {response.status_code}: {error_body} "
                f"| URL: {exchange_url} | X-Revio-Host attached: {bool(REVIO_PSA_HOST)}"
            ),
        )

    jwt_token = _extract_revio_jwt(response_payload)
    if not jwt_token:
        safe_keys = list(response_payload.keys()) if isinstance(response_payload, dict) else type(response_payload).__name__
        REVIO_JWT_CACHE["last_exchange_error"] = f"Token was not found in exchange response. Keys/type: {safe_keys}"
        raise HTTPException(
            status_code=502,
            detail=f"Rev.io API key exchange succeeded, but no JWT token was found in the response. Response keys/type: {safe_keys}",
        )

    exp = _decode_jwt_exp_seconds(jwt_token)
    expires_at = exp if exp else now + 20 * 60
    REVIO_JWT_CACHE.update({
        "token": jwt_token,
        "expires_at": expires_at,
        "last_exchange_error": None,
    })
    return jwt_token


def revio_headers(force_token_refresh: bool = False) -> Dict[str, str]:
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    if REVIO_PSA_HOST:
        # Required by Rev.io PSA tenant routing. This is intentionally X-Revio-Host,
        # not the raw HTTP Host header, because manually overriding Host caused 405s.
        headers["X-Revio-Host"] = REVIO_PSA_HOST.strip()

    if REVIO_PSA_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {REVIO_PSA_BEARER_TOKEN.strip()}"
    elif REVIO_PSA_AUTH_HEADER:
        headers["Authorization"] = REVIO_PSA_AUTH_HEADER.strip()
    elif REVIO_PSA_API_KEY:
        api_key = REVIO_PSA_API_KEY.strip()
        if REVIO_PSA_API_KEY_AUTH_MODE in {"exchange", "jwt", "jwt_exchange", "api_key_exchange"}:
            jwt_token = exchange_revio_api_key_for_jwt(force_refresh=force_token_refresh)
            headers["Authorization"] = f"Bearer {jwt_token}"
        elif REVIO_PSA_API_KEY_AUTH_MODE in {"bearer", "token"}:
            headers["Authorization"] = f"Bearer {api_key}"
        elif REVIO_PSA_API_KEY_AUTH_MODE in {"subscription", "apim", "header"}:
            header_name = (REVIO_PSA_API_KEY_HEADER or "Ocp-Apim-Subscription-Key").strip()
            scheme = (REVIO_PSA_API_KEY_SCHEME or "").strip()
            headers[header_name] = api_key if not scheme else f"{scheme} {api_key}"
        else:
            header_name = (REVIO_PSA_API_KEY_HEADER or "Authorization").strip()
            scheme = (REVIO_PSA_API_KEY_SCHEME or "").strip()
            headers[header_name] = api_key if not scheme else f"{scheme} {api_key}"
    return headers


def revio_auth():
    if REVIO_PSA_API_KEY or REVIO_PSA_AUTH_HEADER or REVIO_PSA_BEARER_TOKEN:
        return None
    if REVIO_PSA_BASIC_USERNAME and REVIO_PSA_BASIC_PASSWORD:
        return (REVIO_PSA_BASIC_USERNAME, REVIO_PSA_BASIC_PASSWORD)
    return None


def revio_is_configured() -> bool:
    has_auth = bool(
        REVIO_PSA_API_KEY
        or REVIO_PSA_AUTH_HEADER
        or REVIO_PSA_BEARER_TOKEN
        or (REVIO_PSA_BASIC_USERNAME and REVIO_PSA_BASIC_PASSWORD)
    )
    has_required_ids = bool(REVIO_TICKET_TYPE_ID and REVIO_TICKET_STATUS_ID and REVIO_TICKET_PRIORITY_ID)
    return bool(REVIO_PSA_BASE_URL and has_auth and has_required_ids)


def get_revio_ticket_url() -> str:
    return f"{REVIO_PSA_BASE_URL}{REVIO_PSA_TICKET_ENDPOINT}"


def get_revio_auth_debug() -> Dict[str, Any]:
    """Return safe Rev.io auth diagnostics without exposing secrets."""
    auth_header_raw = REVIO_PSA_AUTH_HEADER or ""
    auth_header = auth_header_raw.strip()
    scheme = None
    encoded_length = 0
    has_leading_or_trailing_whitespace = auth_header_raw != auth_header
    has_internal_line_break = "\n" in auth_header_raw or "\r" in auth_header_raw
    api_key_header_name = None
    mode = "none"

    if REVIO_PSA_BEARER_TOKEN:
        mode = "REVIO_PSA_BEARER_TOKEN"
        scheme = "Bearer"
        encoded_length = len(REVIO_PSA_BEARER_TOKEN.strip())
        has_leading_or_trailing_whitespace = REVIO_PSA_BEARER_TOKEN != REVIO_PSA_BEARER_TOKEN.strip()
        has_internal_line_break = "\n" in REVIO_PSA_BEARER_TOKEN or "\r" in REVIO_PSA_BEARER_TOKEN
    elif REVIO_PSA_AUTH_HEADER:
        mode = "REVIO_PSA_AUTH_HEADER"
        parts = auth_header.split(" ", 1)
        scheme = parts[0] if parts else None
        if len(parts) > 1:
            encoded_length = len(parts[1].strip())
    elif REVIO_PSA_API_KEY:
        if REVIO_PSA_API_KEY_AUTH_MODE in {"exchange", "jwt", "jwt_exchange", "api_key_exchange"}:
            mode = "REVIO_PSA_API_KEY_EXCHANGE_TO_JWT"
            api_key_header_name = "Authorization"
            scheme = "Bearer"
        elif REVIO_PSA_API_KEY_AUTH_MODE in {"bearer", "token"}:
            mode = "REVIO_PSA_API_KEY_AS_BEARER"
            api_key_header_name = "Authorization"
            scheme = "Bearer"
        else:
            mode = "REVIO_PSA_API_KEY"
            api_key_header_name = (REVIO_PSA_API_KEY_HEADER or "Ocp-Apim-Subscription-Key").strip()
            scheme = (REVIO_PSA_API_KEY_SCHEME or "").strip() or None
        encoded_length = len(REVIO_PSA_API_KEY.strip())
        has_leading_or_trailing_whitespace = REVIO_PSA_API_KEY != REVIO_PSA_API_KEY.strip()
        has_internal_line_break = "\n" in REVIO_PSA_API_KEY or "\r" in REVIO_PSA_API_KEY
    elif REVIO_PSA_BASIC_USERNAME and REVIO_PSA_BASIC_PASSWORD:
        mode = "REVIO_PSA_BASIC_USERNAME_PASSWORD"
        scheme = "Basic"
        encoded_length = 0

    authorization_attached = bool(REVIO_PSA_BEARER_TOKEN or REVIO_PSA_AUTH_HEADER)
    if REVIO_PSA_API_KEY and not authorization_attached:
        authorization_attached = REVIO_PSA_API_KEY_AUTH_MODE in {
            "exchange", "jwt", "jwt_exchange", "api_key_exchange", "bearer", "token"
        } or (REVIO_PSA_API_KEY_HEADER or "Authorization").strip().lower() == "authorization"

    return {
        "auth_mode": mode,
        "authorization_header_attached": authorization_attached,
        "authorization_scheme": scheme,
        "api_key_header_name": api_key_header_name,
        "api_key_header_attached": bool(REVIO_PSA_API_KEY),
        "api_key_auth_mode": REVIO_PSA_API_KEY_AUTH_MODE,
        "auth_exchange_url": get_revio_auth_exchange_url() if REVIO_PSA_BASE_URL else None,
        "auth_exchange_body_field": REVIO_PSA_API_KEY_EXCHANGE_BODY_FIELD,
        "last_exchange_status": REVIO_JWT_CACHE.get("last_exchange_status"),
        "last_exchange_error": REVIO_JWT_CACHE.get("last_exchange_error"),
        "jwt_cached": bool(REVIO_JWT_CACHE.get("token")),
        "jwt_cache_expires_at": REVIO_JWT_CACHE.get("expires_at"),
        "psa_host_set": bool(REVIO_PSA_HOST),
        "psa_host": REVIO_PSA_HOST if REVIO_PSA_HOST else None,
        "x_revio_host_attached": bool(REVIO_PSA_HOST),
        "host_header_attached": False,
        "encoded_value_length": encoded_length,
        "has_leading_or_trailing_whitespace": has_leading_or_trailing_whitespace,
        "has_internal_line_break": has_internal_line_break,
        "basic_username_set": bool(REVIO_PSA_BASIC_USERNAME),
        "basic_password_set": bool(REVIO_PSA_BASIC_PASSWORD),
    }


def get_revio_debug_summary() -> Dict[str, Any]:
    ticket_url = get_revio_ticket_url()
    auth_debug = get_revio_auth_debug()
    base_url_includes_endpoint = "/psac/api/v1/ticket" in (REVIO_PSA_BASE_URL or "").lower()
    endpoint_starts_with_slash = REVIO_PSA_TICKET_ENDPOINT.startswith("/")

    return {
        "configured": revio_is_configured(),
        "base_url_set": bool(REVIO_PSA_BASE_URL),
        "base_url": REVIO_PSA_BASE_URL,
        "endpoint": REVIO_PSA_TICKET_ENDPOINT,
        "final_ticket_url": ticket_url,
        "base_url_appears_to_include_ticket_endpoint": base_url_includes_endpoint,
        "endpoint_starts_with_slash": endpoint_starts_with_slash,
        "ticket_type_id": REVIO_TICKET_TYPE_ID,
        "ticket_status_id": REVIO_TICKET_STATUS_ID,
        "ticket_priority_id": REVIO_TICKET_PRIORITY_ID,
        "include_custom_fields": REVIO_INCLUDE_CUSTOM_FIELDS,
        **auth_debug,
    }


def build_revio_ticket_payload(call_log: Dict[str, Any]) -> Dict[str, Any]:
    duration_text = format_duration_seconds(int(call_log.get("duration_seconds") or 0))
    user_label = call_log.get("display_name") or call_log.get("email") or call_log.get("person_id")
    remote = call_log.get("remote_name") or call_log.get("remote_number") or "Unknown remote party"
    org = call_log.get("org_name") or call_log.get("org_id") or "Unknown organization"

    note = call_log.get("psa_note") or call_log.get("call_note") or ""
    note_text = f" Note: {note}." if note else ""

    full_details = (
        f"Webex call time logged for {user_label}. "
        f"Total call time: {duration_text}. "
        f"Started: {call_log.get('started_at')}. Ended: {call_log.get('ended_at')}. "
        f"Organization: {org}. Remote party: {remote}. "
        f"Call ID: {call_log.get('call_id')}. Session ID: {call_log.get('call_session_id')}."
        f"{note_text}"
    )

    # Keep ticketDescription intentionally short for PSA compatibility. Put the
    # longer call details in workRequested where longer operational notes belong.
    short_description = f"Webex call logged: {user_label} - {duration_text}"
    if len(short_description) > 145:
        short_description = short_description[:142] + "..."

    payload: Dict[str, Any] = {
        "ticketDescription": short_description,
        "ticketTypeId": REVIO_TICKET_TYPE_ID,
        "ticketStatusId": REVIO_TICKET_STATUS_ID,
        "ticketPriorityId": REVIO_TICKET_PRIORITY_ID,
        "workRequested": full_details,
    }

    if REVIO_INCLUDE_CUSTOM_FIELDS:
        payload["customFields"] = [
            {"key": "webexUserEmail", "value": call_log.get("email")},
            {"key": "webexUserDisplayName", "value": call_log.get("display_name")},
            {"key": "webexOrgName", "value": org},
            {"key": "webexRemoteName", "value": call_log.get("remote_name")},
            {"key": "webexRemoteNumber", "value": call_log.get("remote_number")},
            {"key": "webexCallId", "value": call_log.get("call_id")},
            {"key": "webexCallSessionId", "value": call_log.get("call_session_id")},
            {"key": "webexCallDurationSeconds", "value": int(call_log.get("duration_seconds") or 0)},
            {"key": "webexCallDuration", "value": duration_text},
        ]

    if REVIO_TICKET_CUSTOMER_ID > 0:
        payload["customerId"] = REVIO_TICKET_CUSTOMER_ID
    if REVIO_TICKET_SEVERITY_ID > 0:
        payload["ticketSeverityId"] = REVIO_TICKET_SEVERITY_ID
    if REVIO_TICKET_USER_GROUP_TARGET:
        payload["userGroupTarget"] = REVIO_TICKET_USER_GROUP_TARGET

    return payload


def extract_ticket_id(response_payload: Any) -> Optional[str]:
    if not isinstance(response_payload, dict):
        return None
    for key in ["ticketId", "id", "TicketId", "ticketNumber", "number"]:
        if response_payload.get(key) is not None:
            return str(response_payload.get(key))
    return None


def send_revio_ticket_for_call_log(call_log_id: int, note: Optional[str] = None) -> Dict[str, Any]:
    if not revio_is_configured():
        raise HTTPException(
            status_code=500,
            detail=(
                "Rev.io PSA is not fully configured. Set REVIO_PSA_BASE_URL, authentication, "
                "REVIO_TICKET_TYPE_ID, REVIO_TICKET_STATUS_ID, and REVIO_TICKET_PRIORITY_ID."
            ),
        )

    with db() as conn:
        if note is not None:
            conn.execute(
                "UPDATE call_logs SET psa_note = ?, call_note = ?, updated_at = ? WHERE id = ?",
                (note, note, now_iso(), call_log_id),
            )
        row = conn.execute("SELECT * FROM call_logs WHERE id = ?", (call_log_id,)).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Call log was not found.")

    call_log = dict(row)
    payload = build_revio_ticket_payload(call_log)
    url = get_revio_ticket_url()

    headers = revio_headers()
    debug_summary = get_revio_debug_summary()
    print("Rev.io PSA ticket request debug:")
    print(json.dumps({
        "url": debug_summary.get("final_ticket_url"),
        "auth_mode": debug_summary.get("auth_mode"),
        "authorization_scheme": debug_summary.get("authorization_scheme"),
        "authorization_header_attached": debug_summary.get("authorization_header_attached"),
        "api_key_header_name": debug_summary.get("api_key_header_name"),
        "api_key_header_attached": debug_summary.get("api_key_header_attached"),
        "psa_host": debug_summary.get("psa_host"),
        "x_revio_host_attached": debug_summary.get("x_revio_host_attached"),
        "host_header_attached": debug_summary.get("host_header_attached"),
        "encoded_value_length": debug_summary.get("encoded_value_length"),
        "last_exchange_status": debug_summary.get("last_exchange_status"),
        "ticket_type_id": REVIO_TICKET_TYPE_ID,
        "ticket_status_id": REVIO_TICKET_STATUS_ID,
        "ticket_priority_id": REVIO_TICKET_PRIORITY_ID,
    }, indent=2))

    response = requests.post(
        url,
        headers=headers,
        auth=revio_auth(),
        json=payload,
        timeout=30,
    )

    # If a cached JWT was rejected, refresh once and retry before surfacing the failure.
    if response.status_code == 401 and REVIO_PSA_API_KEY and REVIO_PSA_API_KEY_AUTH_MODE in {"exchange", "jwt", "jwt_exchange", "api_key_exchange"}:
        print("Rev.io PSA ticket returned 401. Refreshing exchanged JWT and retrying once.")
        headers = revio_headers(force_token_refresh=True)
        debug_summary = get_revio_debug_summary()
        response = requests.post(
            url,
            headers=headers,
            auth=revio_auth(),
            json=payload,
            timeout=30,
        )

    try:
        response_payload = response.json() if response.text else {}
    except Exception:
        response_payload = {"raw": response.text}

    ts = now_iso()
    if response.status_code in (200, 201, 202):
        ticket_id = extract_ticket_id(response_payload)
        with db() as conn:
            conn.execute(
                """
                UPDATE call_logs
                SET psa_ticket_status = 'sent', psa_ticket_id = ?, psa_error = NULL, updated_at = ?
                WHERE id = ?
                """,
                (ticket_id, ts, call_log_id),
            )
        return {"success": True, "ticket_id": ticket_id, "status_code": response.status_code, "response": response_payload}

    error_text = (
        f"{response.status_code}: {response.text[:1000]} "
        f"| URL: {url} "
        f"| Auth mode: {debug_summary.get('auth_mode')} "
        f"| Auth scheme: {debug_summary.get('authorization_scheme')} "
        f"| Auth header attached: {debug_summary.get('authorization_header_attached')} "
        f"| API key header: {debug_summary.get('api_key_header_name')} "
        f"| API key attached: {debug_summary.get('api_key_header_attached')} "
        f"| PSA host: {debug_summary.get('psa_host')} "
        f"| X-Revio-Host attached: {debug_summary.get('x_revio_host_attached')} "
        f"| Host header attached: {debug_summary.get('host_header_attached')} "
        f"| Encoded length: {debug_summary.get('encoded_value_length')}"
    )
    with db() as conn:
        conn.execute(
            """
            UPDATE call_logs
            SET psa_ticket_status = 'failed', psa_error = ?, updated_at = ?
            WHERE id = ?
            """,
            (error_text, ts, call_log_id),
        )
    raise HTTPException(status_code=502, detail=f"Rev.io ticket creation failed. Rev.io said: {error_text}")



def get_zoho_insert_url() -> str:
    return f"{ZOHO_CRM_BASE_URL}/crm/{ZOHO_CRM_API_VERSION}/{ZOHO_CRM_MODULE}"


def get_zoho_call_notes_url(call_record_id: str) -> str:
    return f"{ZOHO_CRM_BASE_URL}/crm/{ZOHO_CRM_API_VERSION}/{ZOHO_CRM_MODULE}/{call_record_id}/Notes"


def zoho_is_configured() -> bool:
    return bool(
        ZOHO_CRM_BASE_URL
        and ZOHO_CRM_MODULE
        and (
            ZOHO_ACCESS_TOKEN
            or (ZOHO_CLIENT_ID and ZOHO_CLIENT_SECRET and ZOHO_REFRESH_TOKEN)
        )
    )


def refresh_zoho_access_token(force_refresh: bool = False) -> str:
    """Return a Zoho CRM access token, refreshing from the refresh token when configured."""
    now = int(time.time())
    cached_token = ZOHO_TOKEN_CACHE.get("token")
    cached_expires_at = int(ZOHO_TOKEN_CACHE.get("expires_at") or 0)
    if not force_refresh and cached_token and cached_expires_at > now + ZOHO_ACCESS_TOKEN_SAFETY_SECONDS:
        return str(cached_token)

    if ZOHO_REFRESH_TOKEN and ZOHO_CLIENT_ID and ZOHO_CLIENT_SECRET:
        token_url = f"{ZOHO_ACCOUNTS_BASE_URL}/oauth/v2/token"
        payload = {
            "refresh_token": ZOHO_REFRESH_TOKEN.strip(),
            "client_id": ZOHO_CLIENT_ID.strip(),
            "client_secret": ZOHO_CLIENT_SECRET.strip(),
            "grant_type": "refresh_token",
        }
        try:
            response = requests.post(token_url, data=payload, timeout=30)
        except Exception as exc:
            ZOHO_TOKEN_CACHE.update({"last_refresh_status": "exception", "last_refresh_error": str(exc)})
            raise HTTPException(status_code=502, detail=f"Zoho token refresh request failed: {exc}")

        ZOHO_TOKEN_CACHE["last_refresh_status"] = response.status_code
        try:
            data = response.json() if response.text else {}
        except Exception:
            data = {}

        if response.status_code >= 400 or not data.get("access_token"):
            error_text = response.text[:1500]
            ZOHO_TOKEN_CACHE["last_refresh_error"] = f"{response.status_code}: {error_text}"
            raise HTTPException(status_code=502, detail=f"Zoho token refresh failed: {response.status_code}: {error_text}")

        token = str(data.get("access_token"))
        expires_in = int(data.get("expires_in") or 3600)
        ZOHO_TOKEN_CACHE.update({
            "token": token,
            "expires_at": now + expires_in,
            "last_refresh_error": None,
        })
        return token

    if ZOHO_ACCESS_TOKEN:
        return ZOHO_ACCESS_TOKEN.strip()

    raise HTTPException(status_code=500, detail="Zoho CRM is not configured. Set ZOHO_REFRESH_TOKEN with client ID/secret, or set ZOHO_ACCESS_TOKEN.")


def zoho_headers(force_token_refresh: bool = False) -> Dict[str, str]:
    token = refresh_zoho_access_token(force_refresh=force_token_refresh)
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Zoho-oauthtoken {token}",
    }




def format_zoho_datetime(value: Any) -> Optional[str]:
    """Return Zoho-friendly ISO8601 datetime without microseconds.

    Webex call logs are stored in UTC. Zoho displays DateTime values in the
    Zoho CRM user's/account timezone. In display_wall_clock mode, we compensate
    for that display timezone so the time shown in Zoho matches the local
    business timezone configured by ZOHO_TIMEZONE.

    Example with defaults:
      Webex UTC: 2026-05-21T19:17:43+00:00
      ZOHO_TIMEZONE=America/Detroit -> intended display: 2026-05-21 15:17
      ZOHO_CRM_DISPLAY_TIMEZONE=America/Los_Angeles -> send: 2026-05-21T15:17:43-07:00
    """
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        if ZoneInfo:
            try:
                intended_local = dt.astimezone(ZoneInfo(ZOHO_TIMEZONE))
            except Exception as exc:
                print(f"Invalid ZOHO_TIMEZONE '{ZOHO_TIMEZONE}', using UTC: {exc}")
                intended_local = dt.astimezone(timezone.utc)

            if ZOHO_TIME_SEND_MODE in {"local_no_offset", "no_offset", "naive", "local"}:
                return intended_local.replace(tzinfo=None, microsecond=0).isoformat()

            if ZOHO_TIME_SEND_MODE in {"display", "display_wall_clock", "wall_clock", "compensate"}:
                try:
                    crm_display_tz = ZoneInfo(ZOHO_CRM_DISPLAY_TIMEZONE)
                    wall_clock = intended_local.replace(tzinfo=None, microsecond=0)
                    return wall_clock.replace(tzinfo=crm_display_tz).isoformat()
                except Exception as exc:
                    print(f"Invalid ZOHO_CRM_DISPLAY_TIMEZONE '{ZOHO_CRM_DISPLAY_TIMEZONE}', using intended local time: {exc}")

            return intended_local.replace(microsecond=0).isoformat()

        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    except Exception:
        # Last-resort cleanup: strip fractional seconds but preserve offset when possible.
        if "." in text:
            prefix, rest = text.split(".", 1)
            if "+" in rest:
                return prefix + "+" + rest.split("+", 1)[1]
            if "-" in rest:
                return prefix + "-" + rest.rsplit("-", 1)[1]
            return prefix + "+00:00"
        return text


def get_zoho_call_start_time(call_log: Dict[str, Any]) -> str:
    """Return a non-empty Zoho-compatible call start time.

    This protects Zoho sends from malformed/legacy call log rows. If started_at
    cannot be parsed, we fall back to ended_at, then current UTC time. The
    returned value is still passed through format_zoho_datetime so timezone
    handling remains consistent with the configured Zoho settings.
    """
    for field_name in ("started_at", "ended_at"):
        raw_value = call_log.get(field_name)
        parsed = parse_iso_datetime(raw_value)
        if parsed:
            formatted = format_zoho_datetime(parsed.isoformat())
            if formatted:
                return formatted

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()



def get_zoho_call_start_time_candidates(call_log: Dict[str, Any]) -> list[str]:
    """Build multiple Zoho-compatible start time formats for automatic retry.

    Zoho CRM tenants can be picky about DateTime parsing depending on module
    layout, user timezone, and API version. The first value preserves the
    configured timezone behavior. The remaining values are safe fallbacks.
    """
    candidates: list[str] = []

    def add(value: Optional[str]):
        if value and value not in candidates:
            candidates.append(value)

    add(get_zoho_call_start_time(call_log))

    raw_dt = parse_iso_datetime(call_log.get("started_at")) or parse_iso_datetime(call_log.get("ended_at"))
    if raw_dt:
        raw_dt = raw_dt.replace(microsecond=0)

        if ZoneInfo:
            try:
                business_dt = raw_dt.astimezone(ZoneInfo(ZOHO_TIMEZONE)).replace(microsecond=0)
                add(business_dt.replace(tzinfo=None).isoformat())
                add(business_dt.isoformat())
            except Exception:
                pass

        if ZoneInfo:
            try:
                crm_dt = raw_dt.astimezone(ZoneInfo(ZOHO_CRM_DISPLAY_TIMEZONE)).replace(microsecond=0)
                add(crm_dt.isoformat())
            except Exception:
                pass

        # Standard UTC ISO format as a later fallback only.
        add(raw_dt.astimezone(timezone.utc).isoformat())

    return candidates


def format_zoho_call_duration(duration_seconds: int) -> str:
    """Return Zoho Calls duration in MM:SS format.

    Testing showed Zoho renders Call_Duration as minutes and seconds
    (for example, 00:17 displays as 00 minutes 17 seconds). So this sends
    the actual completed call length as MM:SS instead of rounding up to
    HH:mm. For calls longer than 59 minutes, the minutes value can exceed 59.
    """
    seconds = max(0, int(duration_seconds or 0))
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    return f"{minutes:02d}:{remaining_seconds:02d}"


def normalize_call_direction(value: Optional[str]) -> Optional[str]:
    """Normalize direction values to Zoho-friendly Inbound/Outbound.

    This is intentionally used for Zoho only. Rev.io PSA payloads are not
    changed by this value.
    """
    if not value:
        return None
    raw = str(value).strip().lower()
    if raw in {"outbound", "outgoing", "originated", "originating", "placed", "dialed", "dialing"}:
        return "Outbound"
    if raw in {"inbound", "incoming", "received", "offered", "terminating"}:
        return "Inbound"
    if "out" in raw:
        return "Outbound"
    if "in" in raw:
        return "Inbound"
    return None


def infer_call_direction_from_event(
    event: Dict[str, Any],
    new_status: Optional[str] = None,
    existing_direction: Optional[str] = None,
) -> Optional[str]:
    """Infer call direction from Webex webhook fields and prior agent state.

    Webex webhook payloads can vary by event/state. The safest approach is:
      1. Preserve a known direction for the same active call.
      2. Use explicit direction fields if present.
      3. Treat the app's Outbound status as outbound.
      4. Treat received/offered/ringing events as inbound only if no known
         outbound direction already exists.
    """
    preserved = normalize_call_direction(existing_direction)
    if preserved and new_status != "Not On Call":
        return preserved

    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    remote = extract_remote_party(event)

    webhook_event = str(event.get("event", "")).lower()
    state = str(data.get("state", "")).lower()
    event_type = str(data.get("eventType", "")).lower()

    candidate_values = [
        data.get("direction"),
        data.get("callDirection"),
        data.get("originatingType"),
        data.get("personality"),
        data.get("callType"),
        data.get("origin"),
        data.get("legType"),
        data.get("callLegType"),
        remote.get("direction") if isinstance(remote, dict) else None,
        remote.get("callDirection") if isinstance(remote, dict) else None,
        remote.get("type") if isinstance(remote, dict) else None,
    ]
    normalized_candidates = [normalize_call_direction(candidate) for candidate in candidate_values]
    if "Outbound" in normalized_candidates:
        return "Outbound"
    if new_status == "Outbound":
        return "Outbound"
    if event_type in {"initiated", "originated", "dialing", "outgoing", "placed", "makecall", "make_call"} or state in {"dialing", "originating", "outgoing"}:
        return "Outbound"

    if preserved:
        return preserved

    if "Inbound" in normalized_candidates:
        return "Inbound"

    if webhook_event == "created" and event_type in {"received", "offered"}:
        return "Inbound"

    if event_type in {"received", "offered"} or state in {"alerting", "ringing"}:
        return "Inbound"

    return preserved

def build_zoho_call_payload(call_log: Dict[str, Any]) -> Dict[str, Any]:
    duration_seconds = int(call_log.get("duration_seconds") or 0)
    duration_text = format_duration_seconds(duration_seconds)
    user_label = call_log.get("display_name") or call_log.get("email") or call_log.get("person_id") or "Unknown user"
    email = call_log.get("email") or ""
    org = call_log.get("org_name") or call_log.get("org_id") or "Unknown organization"
    remote = call_log.get("remote_name") or call_log.get("remote_number") or "Unknown remote party"
    remote_number = call_log.get("remote_number") or ""
    call_direction = normalize_call_direction(call_log.get("call_direction")) or "Inbound"

    subject = f"{ZOHO_CALL_SUBJECT_PREFIX} - {user_label} - {remote}"[:255]
    description = (
        f"Webex completed call log.\n\n"
        f"User: {user_label}\n"
        f"Email: {email}\n"
        f"Organization: {org}\n"
        f"Remote Party: {remote}\n"
        f"Remote Number: {remote_number}\n"
        f"Direction for Zoho: {call_direction}\n"
        f"Duration: {duration_text} ({duration_seconds} seconds)\n"
        f"Zoho Call_Duration Sent: {format_zoho_call_duration(duration_seconds)} (Zoho displays this as minutes and seconds)\n"
        f"Started: {call_log.get('started_at')}\n"
        f"Zoho Call_Start_Time Sent: {get_zoho_call_start_time(call_log)}\n"
        f"Ended: {call_log.get('ended_at')}\n"
        f"Call ID: {call_log.get('call_id')}\n"
        f"Session ID: {call_log.get('call_session_id')}\n"
    )

    record = {
        "Subject": subject,
        "Call_Type": call_direction,
        "Call_Start_Time": get_zoho_call_start_time(call_log),
        "Call_Duration": format_zoho_call_duration(duration_seconds),
        "Description": description,
    }

    # Zoho treats outbound calls without an Outgoing_Call_Status as scheduled,
    # which then requires Who_Id/What_Id. Mark it completed because this app is
    # logging completed Webex calls.
    if call_direction == "Outbound":
        record["Outgoing_Call_Status"] = "Completed"
    else:
        record["Incoming_Call_Status"] = "Completed"

    if ZOHO_EXACT_DURATION_FIELD_API_NAME:
        record[ZOHO_EXACT_DURATION_FIELD_API_NAME] = duration_seconds

    if remote_number:
        record["Phone"] = remote_number[:30]

    if ZOHO_INCLUDE_CUSTOM_FIELDS:
        prefix = ZOHO_CUSTOM_FIELD_PREFIX
        record.update({
            f"{prefix}User_Email": email,
            f"{prefix}User_Name": user_label,
            f"{prefix}Organization": org,
            f"{prefix}Remote_Name": call_log.get("remote_name"),
            f"{prefix}Remote_Number": remote_number,
            f"{prefix}Call_Direction": call_direction,
            f"{prefix}Duration_Seconds": duration_seconds,
            f"{prefix}Call_ID": call_log.get("call_id"),
            f"{prefix}Session_ID": call_log.get("call_session_id"),
        })

    # Remove empty values so Zoho does not reject optional fields with nulls.
    record = {k: v for k, v in record.items() if v not in (None, "")}
    return {"data": [record]}


def _extract_zoho_record_id(response_payload: Any) -> Optional[str]:
    try:
        data = response_payload.get("data") if isinstance(response_payload, dict) else None
        if isinstance(data, list) and data:
            details = data[0].get("details") if isinstance(data[0], dict) else None
            if isinstance(details, dict) and details.get("id"):
                return str(details.get("id"))
            if isinstance(data[0], dict) and data[0].get("id"):
                return str(data[0].get("id"))
    except Exception:
        pass
    return None


def create_zoho_note_for_call(call_record_id: str, note: str) -> Dict[str, Any]:
    """Create a note under the Zoho Calls record.

    This uses Zoho's related Notes endpoint so the user's note lands in the
    Notes section of the call instead of the call Description field.
    """
    clean_note = (note or "").strip()
    if not call_record_id or not clean_note:
        return {"created": False, "skipped": True}

    url = get_zoho_call_notes_url(call_record_id)
    payload = {
        "data": [
            {
                "Note_Title": "Webex Call Note",
                "Note_Content": clean_note,
            }
        ]
    }

    def _post(force_refresh: bool = False):
        return requests.post(url, headers=zoho_headers(force_token_refresh=force_refresh), json=payload, timeout=30)

    response = _post(False)
    if response.status_code == 401 and ZOHO_REFRESH_TOKEN:
        response = _post(True)

    try:
        response_payload = response.json() if response.text else {}
    except Exception:
        response_payload = {"raw": response.text[:4000]}

    record_status = None
    record_message = None
    note_id = None
    if isinstance(response_payload, dict) and isinstance(response_payload.get("data"), list) and response_payload["data"]:
        first = response_payload["data"][0]
        if isinstance(first, dict):
            record_status = str(first.get("status") or "")
            record_message = first.get("message")
            details = first.get("details") if isinstance(first.get("details"), dict) else {}
            note_id = details.get("id") or first.get("id")

    ok = response.status_code < 400 and str(record_status).lower() not in {"error", "failure", "failed"}
    if ok:
        return {"created": True, "note_id": str(note_id) if note_id else None, "response": response_payload}

    error_text = f"{response.status_code}: {record_message or 'Zoho note creation failed'} | {json.dumps(response_payload)[:1500]}"
    raise HTTPException(status_code=502, detail=f"Zoho call was created, but the Zoho Note failed. Zoho said: {error_text}")


def send_zoho_record_for_call_log(call_log_id: int, direction_override: Optional[str] = None, note: Optional[str] = None) -> Dict[str, Any]:
    if not zoho_is_configured():
        raise HTTPException(status_code=500, detail="Zoho CRM is not configured. Add Zoho OAuth variables in Render.")

    with db() as conn:
        if note is not None:
            conn.execute(
                "UPDATE call_logs SET zoho_note = ?, call_note = ?, updated_at = ? WHERE id = ?",
                (note, note, now_iso(), call_log_id),
            )
        row = conn.execute("SELECT * FROM call_logs WHERE id = ?", (call_log_id,)).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Completed call log was not found.")

    call_log = dict(row)
    if direction_override:
        normalized_override = normalize_call_direction(direction_override)
        if normalized_override:
            call_log["call_direction"] = normalized_override
    payload = build_zoho_call_payload(call_log)
    url = get_zoho_insert_url()

    def _post(current_payload: Dict[str, Any], force_refresh: bool = False):
        return requests.post(url, headers=zoho_headers(force_token_refresh=force_refresh), json=current_payload, timeout=30)

    def _parse_response(resp: requests.Response) -> Dict[str, Any]:
        try:
            return resp.json() if resp.text else {}
        except Exception:
            return {"raw": resp.text[:4000]}

    def _is_call_start_time_error(resp_payload: Any) -> bool:
        try:
            data = resp_payload.get("data") if isinstance(resp_payload, dict) else None
            if not isinstance(data, list):
                return False
            for item in data:
                if not isinstance(item, dict):
                    continue
                details = item.get("details") if isinstance(item.get("details"), dict) else {}
                message = str(item.get("message") or "").lower()
                if details.get("api_name") == "Call_Start_Time" or "call start time" in message:
                    return True
        except Exception:
            return False
        return False

    response = None
    response_payload: Any = {}
    attempted_start_times: list[str] = []

    try:
        start_time_candidates = get_zoho_call_start_time_candidates(call_log)
        if not start_time_candidates:
            start_time_candidates = [datetime.now(timezone.utc).replace(microsecond=0).isoformat()]

        for index, start_time_value in enumerate(start_time_candidates):
            payload["data"][0]["Call_Start_Time"] = start_time_value
            attempted_start_times.append(start_time_value)

            response = _post(payload, False)
            if response.status_code == 401 and ZOHO_REFRESH_TOKEN:
                response = _post(payload, True)

            response_payload = _parse_response(response)

            # Stop immediately unless Zoho specifically rejected Call_Start_Time.
            if not _is_call_start_time_error(response_payload):
                break

            print(f"Zoho rejected Call_Start_Time candidate {index + 1}: {start_time_value}")

    except HTTPException:
        raise
    except Exception as exc:
        error_text = f"Zoho CRM request failed before reaching Zoho: {exc}"
        with db() as conn:
            conn.execute(
                "UPDATE call_logs SET zoho_status = 'failed', zoho_error = ?, updated_at = ? WHERE id = ?",
                (error_text, now_iso(), call_log_id),
            )
        raise HTTPException(status_code=502, detail=error_text)

    if response is None:
        error_text = "Zoho CRM request was not attempted because no response object was created."
        with db() as conn:
            conn.execute(
                "UPDATE call_logs SET zoho_status = 'failed', zoho_error = ?, updated_at = ? WHERE id = ?",
                (error_text, now_iso(), call_log_id),
            )
        raise HTTPException(status_code=502, detail=error_text)

    # Zoho Insert Records often returns HTTP 201/202 or 200 with per-record status.
    record_status = None
    record_message = None
    if isinstance(response_payload, dict) and isinstance(response_payload.get("data"), list) and response_payload["data"]:
        first = response_payload["data"][0]
        if isinstance(first, dict):
            record_status = str(first.get("status") or "")
            record_message = first.get("message")

    ok = response.status_code < 400 and record_status.lower() not in {"error", "failure", "failed"}
    record_id = _extract_zoho_record_id(response_payload)
    ts = now_iso()

    if ok:
        note_result = {"created": False, "skipped": True}
        note_text = (note or "").strip()
        if note_text and record_id:
            try:
                note_result = create_zoho_note_for_call(record_id, note_text)
            except HTTPException as exc:
                # The call record already exists, so keep Zoho marked as sent,
                # but surface the note failure clearly.
                note_error = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
                with db() as conn:
                    conn.execute(
                        """
                        UPDATE call_logs
                        SET zoho_status = 'sent', zoho_record_id = ?, zoho_error = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (record_id, note_error, ts, call_log_id),
                    )
                return {
                    "zoho_record_id": record_id,
                    "zoho_response": response_payload,
                    "zoho_note": {"created": False, "error": note_error},
                }

        with db() as conn:
            conn.execute(
                """
                UPDATE call_logs
                SET zoho_status = 'sent', zoho_record_id = ?, zoho_error = NULL, updated_at = ?
                WHERE id = ?
                """,
                (record_id, ts, call_log_id),
            )
        return {"zoho_record_id": record_id, "zoho_response": response_payload, "zoho_note": note_result}

    error_text = f"{response.status_code}: {json.dumps(response_payload)[:1500]} | Attempted Call_Start_Time values: {attempted_start_times}"
    if record_message:
        error_text = f"{response.status_code}: {record_message} | {json.dumps(response_payload)[:1500]} | Attempted Call_Start_Time values: {attempted_start_times}"
    with db() as conn:
        conn.execute(
            "UPDATE call_logs SET zoho_status = 'failed', zoho_error = ?, updated_at = ? WHERE id = ?",
            (error_text, ts, call_log_id),
        )
    raise HTTPException(status_code=502, detail=f"Zoho CRM record creation failed. Zoho said: {error_text}")


def get_zoho_debug() -> Dict[str, Any]:
    return {
        "configured": zoho_is_configured(),
        "base_url": ZOHO_CRM_BASE_URL,
        "accounts_url": ZOHO_ACCOUNTS_BASE_URL,
        "api_version": ZOHO_CRM_API_VERSION,
        "module": ZOHO_CRM_MODULE,
        "final_insert_url": get_zoho_insert_url(),
        "access_token_set": bool(ZOHO_ACCESS_TOKEN),
        "refresh_token_set": bool(ZOHO_REFRESH_TOKEN),
        "client_id_set": bool(ZOHO_CLIENT_ID),
        "client_secret_set": bool(ZOHO_CLIENT_SECRET),
        "include_custom_fields": ZOHO_INCLUDE_CUSTOM_FIELDS,
        "timezone": ZOHO_TIMEZONE,
        "time_send_mode": ZOHO_TIME_SEND_MODE,
        "crm_display_timezone": ZOHO_CRM_DISPLAY_TIMEZONE,
        "last_refresh_status": ZOHO_TOKEN_CACHE.get("last_refresh_status"),
        "last_refresh_error": ZOHO_TOKEN_CACHE.get("last_refresh_error"),
        "token_cached": bool(ZOHO_TOKEN_CACHE.get("token")),
    }

def create_call_log_if_completed(conn: Any, existing: Any, ended_at: str) -> Optional[int]:
    previous_status = existing["status"]
    if previous_status not in {"On Call", "Outbound"}:
        return None

    if not is_authenticated_agent_row(existing):
        return None

    started_at = existing["state_started_at"] or existing["updated_at"]
    start_dt = parse_iso_datetime(started_at)
    end_dt = parse_iso_datetime(ended_at)
    if not start_dt or not end_dt:
        return None

    duration_seconds = int(max(0, (end_dt - start_dt).total_seconds()))
    if duration_seconds <= 0:
        return None

    call_direction = normalize_call_direction(existing["call_direction"] if "call_direction" in existing.keys() else None) or ("Outbound" if previous_status == "Outbound" else "Inbound")

    # Avoid duplicate call logs for the same completed call/session.
    call_session_id = existing["call_session_id"]
    call_id = existing["call_id"]
    duplicate = None
    if call_session_id:
        duplicate = conn.execute(
            "SELECT id FROM call_logs WHERE call_session_id = ?",
            (call_session_id,),
        ).fetchone()
    elif call_id:
        duplicate = conn.execute(
            "SELECT id FROM call_logs WHERE call_id = ? AND person_id = ?",
            (call_id, existing["person_id"]),
        ).fetchone()

    if duplicate:
        return duplicate["id"]

    insert_sql = """
        INSERT INTO call_logs (
            person_id, email, display_name, org_id, org_name, call_id, call_session_id,
            remote_name, remote_number, remote_call_type, call_direction, started_at, ended_at,
            duration_seconds, psa_ticket_status, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'not_sent', ?, ?)
    """
    if USE_POSTGRES:
        insert_sql += " RETURNING id"

    cursor = conn.execute(
        insert_sql,
        (
            existing["person_id"], existing["email"], existing["display_name"],
            existing["org_id"], existing["org_name"], call_id, call_session_id,
            existing["remote_name"], existing["remote_number"], existing["remote_call_type"], call_direction,
            started_at, ended_at, duration_seconds, ended_at, ended_at,
        ),
    )
    if USE_POSTGRES:
        return int(cursor.fetchone()["id"])
    return int(cursor.lastrowid)


def cleanup_unauthenticated_agents(conn: Any) -> int:
    """Remove old hidden webhook-only placeholder users.

    These rows are created when Webex sends a call event before that person has
    authenticated through /oauth/start. They are hidden from the main dashboard,
    so keeping them forever only wastes database space.
    """
    if UNAUTHENTICATED_AGENT_RETENTION_SECONDS <= 0:
        return 0

    cutoff = datetime.fromtimestamp(
        time.time() - UNAUTHENTICATED_AGENT_RETENTION_SECONDS,
        tz=timezone.utc,
    ).isoformat()

    cursor = conn.execute("""
        DELETE FROM agents
        WHERE (access_token IS NULL OR access_token = '')
          AND (refresh_token IS NULL OR refresh_token = '')
          AND (email IS NULL OR email = '' OR email = person_id)
          AND updated_at < ?
    """, (cutoff,))
    return cursor.rowcount or 0


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
    if USE_POSTGRES:
        init_postgres_db()
    else:
        init_sqlite_db()


def init_postgres_db():
    """Create the PostgreSQL schema used by the attendant console."""
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
                call_direction TEXT,
                state_started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                webhook_id TEXT,
                access_token TEXT,
                refresh_token TEXT,
                token_expires_at TEXT,
                dnd_enabled TEXT,
                dnd_ring_reminder TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS call_logs (
                id SERIAL PRIMARY KEY,
                person_id TEXT NOT NULL,
                email TEXT,
                display_name TEXT,
                org_id TEXT,
                org_name TEXT,
                call_id TEXT,
                call_session_id TEXT,
                remote_name TEXT,
                remote_number TEXT,
                remote_call_type TEXT,
                call_direction TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                duration_seconds INTEGER NOT NULL,
                psa_ticket_status TEXT NOT NULL DEFAULT 'not_sent',
                psa_ticket_id TEXT,
                psa_error TEXT,
                zoho_status TEXT NOT NULL DEFAULT 'not_sent',
                zoho_record_id TEXT,
                zoho_error TEXT,
                call_note TEXT,
                psa_note TEXT,
                zoho_note TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # Safe forward-compatible column additions for existing databases.
        for table, columns in {
            "agents": [
                "extension TEXT", "org_id TEXT", "org_name TEXT", "access_token TEXT",
                "refresh_token TEXT", "token_expires_at TEXT", "dnd_enabled TEXT", "dnd_ring_reminder TEXT", "call_direction TEXT",
            ],
            "events": ["org_id TEXT", "org_name TEXT"],
            "call_logs": ["psa_ticket_status TEXT", "psa_ticket_id TEXT", "psa_error TEXT", "zoho_status TEXT", "zoho_record_id TEXT", "zoho_error TEXT", "call_direction TEXT", "call_note TEXT", "psa_note TEXT", "zoho_note TEXT"],
        }.items():
            for col_def in columns:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_def}")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_person_id ON events (person_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agents_updated_at ON agents (updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_call_logs_ended_at ON call_logs (ended_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_call_logs_person_id ON call_logs (person_id)")


def init_sqlite_db():
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
                call_direction TEXT,
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS call_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id TEXT NOT NULL,
                email TEXT,
                display_name TEXT,
                org_id TEXT,
                org_name TEXT,
                call_id TEXT,
                call_session_id TEXT,
                remote_name TEXT,
                remote_number TEXT,
                remote_call_type TEXT,
                call_direction TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                duration_seconds INTEGER NOT NULL,
                psa_ticket_status TEXT NOT NULL DEFAULT 'not_sent',
                psa_ticket_id TEXT,
                psa_error TEXT,
                zoho_status TEXT NOT NULL DEFAULT 'not_sent',
                zoho_record_id TEXT,
                zoho_error TEXT,
                call_note TEXT,
                psa_note TEXT,
                zoho_note TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        call_log_columns = {row["name"] for row in conn.execute("PRAGMA table_info(call_logs)").fetchall()}
        for column_name in ["psa_ticket_status", "psa_ticket_id", "psa_error", "zoho_status", "zoho_record_id", "zoho_error", "call_direction", "call_note", "psa_note", "zoho_note"]:
            if column_name not in call_log_columns:
                conn.execute(f"ALTER TABLE call_logs ADD COLUMN {column_name} TEXT")

        agent_columns = {row["name"] for row in conn.execute("PRAGMA table_info(agents)").fetchall()}
        for column_name in ["extension", "org_id", "org_name", "access_token", "refresh_token", "token_expires_at", "dnd_enabled", "dnd_ring_reminder", "call_direction"]:
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
        conn.execute("DELETE FROM call_logs WHERE person_id = ?", (person_id,))


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
    call_direction = None

    if new_status == "Not On Call":
        webex_state = None
        event_type = None
        call_id = None
        call_session_id = None
        remote_name = None
        remote_number = None
        remote_call_type = None
        call_direction = None
    else:
        remote_name = remote.get("name")
        remote_number = remote.get("number")
        remote_call_type = remote.get("callType")

    ts = now_iso()
    completed_call_log_id = None

    with db() as conn:
        existing = conn.execute(
            "SELECT * FROM agents WHERE person_id = ?",
            (person_id,),
        ).fetchone()

        if existing:
            if new_status != "Not On Call":
                call_direction = infer_call_direction_from_event(event, new_status, existing["call_direction"] if "call_direction" in existing.keys() else None)

            if new_status == "Not On Call" and existing["status"] != "Not On Call":
                completed_call_log_id = create_call_log_if_completed(conn, existing, ts)

            state_started_at = existing["state_started_at"] if existing["status"] == new_status else ts

            if org_name == org_id and existing["org_name"] and existing["org_name"] != existing["org_id"]:
                org_name = existing["org_name"]

            conn.execute("""
                UPDATE agents
                SET status = ?, extension = COALESCE(?, extension),
                    org_id = COALESCE(?, org_id), org_name = COALESCE(?, org_name),
                    webex_state = ?, event_type = ?, call_id = ?, call_session_id = ?,
                    remote_name = ?, remote_number = ?, remote_call_type = ?, call_direction = ?,
                    state_started_at = ?, updated_at = ?, webhook_id = ?
                WHERE person_id = ?
            """, (
                new_status, extension, org_id, org_name,
                webex_state, event_type, call_id, call_session_id,
                remote_name, remote_number, remote_call_type, call_direction,
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
                    remote_name, remote_number, remote_call_type, call_direction,
                    state_started_at, updated_at, webhook_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                person_id, None, None, extension, org_id, org_name, new_status,
                webex_state, event_type, call_id, call_session_id,
                remote_name, remote_number, remote_call_type, infer_call_direction_from_event(event, new_status),
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
        cleanup_unauthenticated_agents(conn)

        row = conn.execute(
            "SELECT * FROM agents WHERE person_id = ?",
            (person_id,),
        ).fetchone()
        row_dict = dict(row)

    if REVIO_TICKET_AUTO_CREATE_ON_CALL_END and completed_call_log_id:
        try:
            send_revio_ticket_for_call_log(completed_call_log_id)
        except Exception as exc:
            print(f"Auto Rev.io ticket creation failed for call_log_id={completed_call_log_id}: {exc}")

    return row_dict


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
        "database_backend": "postgres" if USE_POSTGRES else "sqlite",
        "database": "DATABASE_URL" if USE_POSTGRES else str(DB_PATH),
        "database_url_set": bool(DATABASE_URL),
        "has_client_id": bool(WEBEX_CLIENT_ID),
        "has_redirect_uri": bool(WEBEX_REDIRECT_URI),
        "has_webhook_target": bool(WEBEX_WEBHOOK_TARGET_URL),
        "has_admin_token": bool(WEBEX_ADMIN_TOKEN),
        "stale_status_after_seconds": STALE_STATUS_AFTER_SECONDS,
        "max_event_rows": MAX_EVENT_ROWS,
        "webhook_payload_max_chars": WEBHOOK_PAYLOAD_MAX_CHARS,
        "store_raw_webhook_payloads": STORE_RAW_WEBHOOK_PAYLOADS,
        "unauthenticated_agent_retention_seconds": UNAUTHENTICATED_AGENT_RETENTION_SECONDS,
        "auto_clear_events_enabled": AUTO_CLEAR_EVENTS_ENABLED,
        "auto_clear_events_every_seconds": AUTO_CLEAR_EVENTS_EVERY_SECONDS,
        "auto_clear_events_vacuum": AUTO_CLEAR_EVENTS_VACUUM,
        "dnd_endpoint_template": WEBEX_DND_ENDPOINT_TEMPLATE,
        "dnd_default_ring_reminder": WEBEX_DND_DEFAULT_RING_REMINDER,
        "revio_psa_configured": revio_is_configured(),
        "revio_psa_base_url_set": bool(REVIO_PSA_BASE_URL),
        "revio_psa_host_set": bool(REVIO_PSA_HOST),
        "revio_api_key_auth_mode": REVIO_PSA_API_KEY_AUTH_MODE,
        "revio_ticket_type_id": REVIO_TICKET_TYPE_ID,
        "revio_ticket_status_id": REVIO_TICKET_STATUS_ID,
        "revio_ticket_priority_id": REVIO_TICKET_PRIORITY_ID,
        "revio_ticket_auto_create_on_call_end": REVIO_TICKET_AUTO_CREATE_ON_CALL_END,
        "revio_include_custom_fields": REVIO_INCLUDE_CUSTOM_FIELDS,
        "zoho_crm_configured": zoho_is_configured(),
        "zoho_crm_base_url_set": bool(ZOHO_CRM_BASE_URL),
        "zoho_crm_module": ZOHO_CRM_MODULE,
        "revio_auth_mode": get_revio_auth_debug().get("auth_mode"),
        "revio_auth_scheme": get_revio_auth_debug().get("authorization_scheme"),
        "revio_auth_header_attached": get_revio_auth_debug().get("authorization_header_attached"),
        "revio_api_key_header_name": get_revio_auth_debug().get("api_key_header_name"),
        "revio_api_key_header_attached": get_revio_auth_debug().get("api_key_header_attached"),
        "revio_ticket_endpoint": REVIO_PSA_TICKET_ENDPOINT,
        "revio_final_ticket_url": get_revio_ticket_url() if REVIO_PSA_BASE_URL else None,
    }




@app.get("/api/revio/debug")
def api_revio_debug():
    """Safe Rev.io diagnostics. Does not expose the actual auth secret."""
    last_call_log = None
    with db() as conn:
        row = conn.execute(
            """
            SELECT id, ended_at, email, display_name, psa_ticket_status, psa_ticket_id, psa_error, updated_at
            FROM call_logs
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            last_call_log = dict(row)

    return {
        "revio": get_revio_debug_summary(),
        "last_call_log": last_call_log,
        "notes": [
            "This endpoint intentionally does not expose REVIO_PSA_AUTH_HEADER, bearer tokens, usernames, or passwords.",
            "For your Rev.io endpoint, REVIO_PSA_BASE_URL should usually be https://api.psarev.io and REVIO_PSA_TICKET_ENDPOINT should be /psac/api/v1/ticket.",
            "This version supports your working bot variable names: REVIO_PSA_API_KEY, REVIO_PSA_BASE_URL, REVIO_PSA_HOST, and REVIO_PSA_TICKET_* IDs.",
            "By default, REVIO_PSA_API_KEY is exchanged for a JWT, then tickets use Authorization: Bearer <JWT>. REVIO_PSA_HOST is sent as X-Revio-Host, not the raw Host header.",
            "If the exchange fails, check last_exchange_status/last_exchange_error. If ticket creation fails after exchange, verify the X-Revio-Host tenant and ticket permissions.",
        ],
    }


@app.get("/api/revio/debug/sample-payload/{call_log_id}")
def api_revio_debug_sample_payload(call_log_id: int):
    """Show the exact Rev.io ticket payload for a call log without sending it."""
    with db() as conn:
        row = conn.execute("SELECT * FROM call_logs WHERE id = ?", (call_log_id,)).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Call log was not found.")

    return {
        "url": get_revio_ticket_url(),
        "auth_debug": get_revio_auth_debug(),
        "payload": build_revio_ticket_payload(dict(row)),
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
    .column-filter-wrap { position: relative; display: inline-block; }
    .column-filter-panel {
      display: none;
      position: absolute;
      right: 0;
      top: calc(100% + 8px);
      z-index: 50;
      width: 280px;
      max-height: 430px;
      overflow: auto;
      background: white;
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: 0 18px 40px rgba(15, 23, 42, 0.18);
      padding: 12px;
    }
    .column-filter-panel.open { display: block; }
    .column-filter-title {
      font-weight: 900;
      color: var(--navy);
      margin-bottom: 8px;
      font-size: 13px;
    }
    .column-option {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 7px 6px;
      border-radius: 8px;
      font-size: 13px;
      color: #334155;
    }
    .column-option:hover { background: #f8fafc; }
    .column-option input {
      min-width: 0;
      width: auto;
      padding: 0;
      margin: 0;
    }
    .column-filter-actions {
      display: flex;
      gap: 6px;
      margin-top: 10px;
      padding-top: 10px;
      border-top: 1px solid var(--border);
    }
    .column-filter-actions button { flex: 1; }
    .saved-view-wrap {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
    }
    .saved-view-wrap select {
      min-width: 190px;
    }
    .detail-button {
      background: #0f766e;
    }
    .call-detail-grid {
      display: grid;
      grid-template-columns: 160px 1fr;
      gap: 8px 12px;
      margin-top: 12px;
      font-size: 14px;
    }
    .call-detail-label {
      color: var(--muted);
      font-weight: 800;
    }
    .call-detail-value {
      color: var(--navy);
      word-break: break-word;
    }
    .call-detail-section-title {
      margin-top: 16px;
      font-weight: 900;
      color: var(--navy);
      border-top: 1px solid var(--border);
      padding-top: 12px;
    }
    input, select { border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; font-size: 14px; min-width: 240px; background: white; }
    button, a.button { border: none; border-radius: 10px; background: var(--blue); color: white; padding: 10px 14px; font-size: 14px; cursor: pointer; text-decoration: none; display: inline-block; }
    button.secondary { background: #475569; }
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
    .call-log-item {
      display: grid;
      grid-template-columns: 1.4fr 130px 140px 140px;
      gap: 10px;
      align-items: center;
      padding: 10px 12px;
      background: #f8fafc;
      border: 1px solid #eef2f7;
      border-radius: 12px;
      font-size: 13px;
    }
    .psa-status {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 12px;
      font-weight: 900;
      white-space: nowrap;
      background: #e5e7eb;
      color: #374151;
    }
    .psa-status.sent { background: #dcfce7; color: #166534; }
    .psa-status.failed { background: #fee2e2; color: #991b1b; }
    .psa-status.not_sent { background: #dbeafe; color: #1d4ed8; }
    .action-line {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
      margin-bottom: 6px;
    }
    .zoho-line {
      margin-top: 2px;
    }
    .zoho-actions {
      display: flex;
      align-items: center;
      gap: 6px;
      margin-top: 6px;
      flex-wrap: wrap;
    }
    .zoho-button {
      background: #7c3aed;
      width: auto;
      min-width: 0;
      padding: 8px 10px;
      font-size: 13px;
      line-height: normal;
      white-space: nowrap;
    }
    .zoho-button.outbound { background: #4f46e5; }
    .zoho-button:disabled {
      background: #e5e7eb;
      color: #9ca3af;
      cursor: not-allowed;
    }
    .direction-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 11px;
      font-weight: 900;
      margin-top: 4px;
      background: #f3f4f6;
      color: #374151;
      border: 1px solid #e5e7eb;
    }
    .direction-badge.Outbound {
      background: #eef2ff;
      color: #3730a3;
      border-color: #c7d2fe;
    }
    .direction-badge.Inbound {
      background: #ecfdf5;
      color: #047857;
      border-color: #a7f3d0;
    }
    .org-row td {
      background: #ecfdf3;
      color: #14532d;
      font-weight: 900;
      text-transform: uppercase;
      letter-spacing: .04em;
      font-size: 12px;
      border-top: 1px solid #bbf7d0;
      border-bottom: 1px solid #bbf7d0;
    }
    .org-count { color: #166534; font-weight: 700; text-transform: none; letter-spacing: 0; margin-left: 8px; }
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
      .header-logo { width: 56px; height: 56px; }
      main { padding: 14px; }
      .summary { grid-template-columns: repeat(2, 1fr); }
      .activity-item { grid-template-columns: 1fr; gap: 4px; }
      .call-log-item { grid-template-columns: 1fr; gap: 6px; }
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
    <img class="header-logo" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAHsAAABiCAYAAABwHqwsAAAL8UlEQVR4nO1dO3LbyhI9ZL2c3IHgKuWCA8XGW4Hh4gIEr+BCISPDEUNTKzC8ANaFVnChWMGFcgXgCh65ArwATRlqDoD5YQDLPlUuswfTPS005tfTMzOrqgpjYrZZBSypqNa7wwiqSGG2WfkAlo2kSevbxGwMY5OBYwAfW7I8AUgBpFN4kRL67lHru52Cvm1wauzZZuWhfikfJFmOAKJqvcsGUqkTs81qiVrfNiNzjKpvH5wZm5q/HMBCg/1rtd4lNvXpg6G+P6r1LrKpjw04MTbV6AJ6L+6E22q929rQpw+W9HX+gfZh7qicFGYvDgC+UW1zgRTm+n5xqK8UBjf2bLOKIN9H92FrSU4rLOubWZJjBS5qdtLzfA/gDsBXAA89eT84qC1xz/Mjfup735P3YrZZhRZ0soL/DCmcDHPRkeVsIEM163sHT4R+g2iB+uqrjixPAILm9EpiIBdiIjV8UGMD8Due7UUj1mq9S+mlf+mTSS/aozSP/gHtzfATgAP9zul3gZ+OkS59j2CGJn2L2WYVo/0D7ZLpFEMb2+t4lvY8azX2bLMq0F0D29DkefVBzDarI35+CCJkbQ4T+kDbjK2j5yBwNRoXoWx7UK13rc9QN5dDvMAFurucsod/b0+VYTCmsYO2B1Ma1DQQtj2gbqfrQ5kEhjZ22fHsRjSyJhdlMow6RriiwaMI2w6+vhmGMwxt7LzvefMF0oJDjgn1cwzfZ5tVQh8kZpuVN9usMnT7zgsHeklhcHepwWDqreB9td4VYysBTMOp8pbxMBVDAw6MTct9k+m3HCMeW4EmXI3GQ9QOjd8Jn6dUqwFHxiZnRIDfx+Cfq/UuHVsJDteRKkvUffhfAxXxgHr0e8D5TMDDT9eqj2HmxU+oI1WKAWQbY6wYtBTAjQVRR9Su1axa73JFHTzU3UsEe7OFdz3ev1Hh3NhUu/9nKGYPILHVVJJzJ4b5Bzi56JQmxnCXRga8R9T9oWezT6zWu4JW4N7BbOYQWVFoIIxRs0vo9Zf3qPvDQ1emx2ffRx3XHbBHJf0rri+LThnkm0+hF5r06bePLgVe3KH/KLIdAcRtNfnx2V+irlEh5MOJ9qgDCtLry6IQZaA+PYN6f35frXehIo8TuDZ2CrV+8RQwUPAHj8++h3pkb9rPPgHYXl8WKX9A44utRhmTHKi5jBtfQm1g1mXoBO3BDbp4ABCLarrGR+os7FkFLgdokUJeoaEfn33v8dkvYN/QQN0F/Pv47Ef8AQ3efijIiu2oZBdTNXYoMLSP2mEy9Ara98dnP+WJZHBZD+CFYMPi6HBibJrHyhrpljtIyNA5zAP3ZXEjMjjqEf5RUkZkSxlbcFWzY8l897yvG8HQJ5wZnKZ9oSR/eApymApcrnr14QhWG2jEncO9oU+4eXz242YCtTp3ErwLyH8YTuBq+4+MsRKBwyST5B0S3x6f/YClJZBrzmPbypjARc0OJfLsBc13gumEM6XkvAHw0pwnEnxX5JyZBAY1Nv2hMhvZkyZBzXdsXSF9XIDpQx+nTKx43JvDEYau2ZFEnr3AFZpg/Oab4wt9hE1sJfhC65poYgrG3jYJeqE21rqHQMLoFP1992R2cg5mbHIqyKxupYxObOtiETeCvjuT4IuGUUcNQ9bsSCLPfXMETi8yHEYda4gYnUnwfJzCnLvX2LPNajnbrAIV9x/9YaFE1ozRIabXV3NETYLWrmWmYaFsAbTTJLA9km/dsita3pttVgAtCfZEioSQM1ou4Js6rh6ffe/6sigbaTn6Zx0xOrYpk2ETsHc326z2qNfzM3VVX0NYs6ngEuKB0hXqPU9ph9xQouwnwZqv7HljYyNkdCbB0zrnprWDAvX75pXkAsDfHZsKpdHWjGeCQjluZptVwhMV5tZFkxB4qaaMgNGFJF/ME+h95eh/399NV9LOjE1fkKznKhYMPEJJ3oLRgSTfFBA0CYU48VCQFkN+nJJK5hNCVLMTBf4Fzs8MiSR5C0ZzOVPGQuBgkYlKFa1zhwrlXpg056+MTYJUIz8PDX4f8q1CwWhPsdyx4TG6lOSLNPna+KXBa7aqoCfWhEnzC1a4prLoIQuP0aUkX8joVLFc7bPgXoxNAwXVk/1SRoeSfG9hC6/H6FKSb8Hcp5lG2ZEGz6uarSMgO/2QOODuraNUyBudflALpxLM+IpfBSbGvmfz5FiBt2gSFHr0O4G7TzNFft46SGEOaNfKjNEqhR8YvVQse3JQ3UWKxvtScLkK+WVxqtnKjDhvwk182gcD3l8VIaMzQ/5e6Br7gY2mI9WCm2jbb/XGETA6U+RfqHrU5tR3qE57MkYrFfoHAM6NlWvICHpzNDCHnucqP/3Q/Fj+oEZw+kEtpeqUNOjN0cBclQE48wUr878RlBZkBIwuFPl9lcw6NZt/far8bfjVTlIqm4RmJIrP6FyRf6FS7hzq056C0b4iPyD2gx805IyJktG+howFW+PmMmUgXe4c6gsQB0YvFfnRUmauIWcsHFmkigm804+hj9SaQ92ZkjNa56YcT5BWaMgZC4UgLdCU5TF6sEPqxzpc/kLQ1+Qj6KGLXJDmacrifKWmnF6MeZOA3yToBKNfZZCWC9J8xzooY47xlhsDQVrmWAcdHK8vi7yZ8Kv4GqZ2R0jmWAcdZIK0wLEOTRxkM84xXl/5gffb5COf+i06mSAtdKzDC1RG8HOMOwoOBWlbxzqoYH99WWSC9NBA5oHRSwXevushX2FOa6kqtclntElNjARpmYG8oZHyBAoiMFneLRit0venKgWd+uxEgcdjdKlSIMMHvkuCnBWqYTqukArSIkOZh9MPxb1dD6pbguZAfb0g5JsEn9G5SoECJIK01FDmEPjBvWYKu19awfpcX5LtCI0THXgMmsw8lw+sCtVCGc6OkKKpzdQiUFNBWmIok/+NoSRfrONafTG24j0eL0pZ2F24gPilidLGwoNgbu3B/ISIjNGBBI/2/SOv5tkNg/fVqpDRSqNCAf4S9N25hB6uEAnSthbkZqcfEidVHFGfZZ7qFnbmVKnWu0O13gWob35vw0dmHG0FGtgK0iILck1xJ+irA5hvL35QCMU+XdKemRTY6kGjuy7+i/ZmPWnkzWDuDPnIA+joJXd9dEPjCHF3srUg+0Vux0DviPreEd/G8menu7Ra7/JqvfMBfMa5MXl/FZsqAyAVDNYSjLdAEvErJmhPuqkf/IHFmUeCPD8A+DYvmJHyjVfrXVqtdx6AT2j0z82m3NI1jBdon8uqBtGb4o57y6jlsXHWeczogP5/AnCL+iaCyPZtBFZvEiDjFzA/BOfsJH7HpxPfXV8WcTPB4t92V613cW+uAWD92ojZZhUD+GZB1NkUgzbA+xhu7fgAIOebFgwuh+F4om5xFAxyR4jFG/lGqwUnUNOdwbxGH1H3waWhHG0MdiGMxUvSrd7OJ4vGUVW2jtYc/dL0IY29RN3H2org2KMevG37LnIzAdXkBHqBlG2YxK27g171NIDBgbo53Nq+85J2om5h18jARAwNOLjXayCDAxZvxqM16b9tyGKYjKEBBzFojUtUbM+TP9o49Y+QWpLTxNcpGRpwFHBII9BUg7XPc+ZpyBSha6R9hLoreD/Fq5VdRpdmivnf05z0PdqNXhjo00SbMW+r9W5J3kMV72BprNEAcGnsQCXzaZpC/wc4f9n3Fq8ujhl9RP2xbRtpuYI8z0ydYeDS2J4g7R71qtZZf97sjxvLrp8o/yeb1xbTR/OOZN8C8ARz4gjnuEd9xxfXX7S9aXS4vGX3gNd9456ax5NhvzOWI+qXfnCgXidopYsvgLy4PlueT2okDri7izPE+SCobPzOBGwLTCDwsGOlK238zgXPQ/vamGES1zN2nCdyFtAwApKW9LyHj0fzjA5Xxhb5lw+Mzlt4I5uKqIC8akKPmqSfO7SojjHG3NhX9NAnLAfVohttZfMAy1KRfxS4MrZoHls2iY44ttS+OnKg0CHRHD9l+UqIdS9s62SC1tt/LCNAPZf1iS5aRqoh6j5yifpjSMZc/yUEqLuSkOi8ZX4f4mcg4gFAatEPYAXOpl5/MD7G7LP/wDH+D1mNYgJIYxUqAAAAAElFTkSuQmCC" alt="Bullfrog logo" />
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
      <div class="column-filter-wrap">
        <button class="secondary" onclick="toggleColumnPanel(event)">Fields ▾</button>
        <div id="columnFilterPanel" class="column-filter-panel">
          <div class="column-filter-title">Show / Hide Fields</div>
          <div id="columnFilterOptions"></div>
          <div class="column-filter-actions">
            <button class="small secondary" onclick="showAllColumns()">Show All</button>
            <button class="small secondary" onclick="hideOptionalColumns()">Default View</button>
          </div>
        </div>
      </div>
      <div class="saved-view-wrap">
        <select id="savedViewSelect" onchange="applySavedView(this.value)"></select>
        <button class="secondary" onclick="saveCurrentView()">Save View</button>
        <button class="secondary small" onclick="deleteSavedView()">Delete View</button>
      </div>
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
      <h2>Completed Calls / PSA + Zoho Log</h2>
      <button class="secondary small" onclick="loadCallLogs()">Refresh Call Log</button>
    </div>
    <div id="callLogList" class="activity-list">
      <div class="empty">Loading completed call records...</div>
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

  <div id="callDetailModal" class="modal-backdrop">
    <div class="modal call-detail-modal">
      <h2>Completed Call Details</h2>
      <div id="callDetailContent"></div>
      <div class="modal-actions">
        <button class="secondary" onclick="closeCallDetailModal()">Close</button>
      </div>
    </div>
  </div>

  <div id="sendNoteModal" class="modal-backdrop">
    <div class="modal call-detail-modal">
      <h2 id="sendNoteTitle">Add Call Note</h2>
      <p id="sendNoteText" class="activity-meta"></p>
      <textarea id="sendNoteInput" rows="6" placeholder="Optional notes to include with this PSA ticket or Zoho call record..." style="width:100%; border:1px solid var(--border); border-radius:12px; padding:12px; font-size:14px; resize:vertical;"></textarea>
      <div class="modal-actions">
        <button id="sendNoteConfirmButton">Send</button>
        <button class="secondary" onclick="closeSendNoteModal()">Cancel</button>
      </div>
    </div>
  </div>

</main>

<script>
let agents = [];
let dndChangeInProgress = false;

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
const VISIBLE_COLUMNS_KEY = "webexSupervisorVisibleColumns";
const DEFAULT_VISIBLE_COLUMNS = ["email", "organization", "extension", "call", "dnd", "status", "duration", "remote_name", "remote_number", "transfer", "reset_status"];

function getVisibleColumnKeys() {
  const validKeys = new Set(DEFAULT_COLUMNS.map(c => c.key));
  const allKeys = DEFAULT_COLUMNS.map(c => c.key);
  const saved = localStorage.getItem(VISIBLE_COLUMNS_KEY);
  if (!saved) return DEFAULT_VISIBLE_COLUMNS.filter(k => validKeys.has(k));

  try {
    const parsed = JSON.parse(saved);
    const cleaned = parsed.filter(k => validKeys.has(k));
    return cleaned.length ? cleaned : allKeys;
  } catch {
    return DEFAULT_VISIBLE_COLUMNS.filter(k => validKeys.has(k));
  }
}

function setVisibleColumnKeys(keys) {
  const validKeys = new Set(DEFAULT_COLUMNS.map(c => c.key));
  const cleaned = keys.filter(k => validKeys.has(k));
  localStorage.setItem(VISIBLE_COLUMNS_KEY, JSON.stringify(cleaned));
}

function renderColumnFilterOptions() {
  const container = document.getElementById("columnFilterOptions");
  if (!container) return;
  const visible = new Set(getVisibleColumnKeys());
  container.innerHTML = DEFAULT_COLUMNS.map(col => `
    <label class="column-option">
      <input type="checkbox" value="${col.key}" ${visible.has(col.key) ? "checked" : ""} onchange="toggleColumnVisibility('${col.key}', this.checked)" />
      <span>${col.label}</span>
    </label>
  `).join("");
}

function toggleColumnVisibility(key, checked) {
  const visible = new Set(getVisibleColumnKeys());
  if (checked) visible.add(key);
  else visible.delete(key);

  // Prevent hiding every column, otherwise the table becomes unusable.
  if (visible.size === 0) {
    visible.add(key);
  }

  setVisibleColumnKeys([...visible]);
  renderColumnFilterOptions();
  renderTable();
}

function showAllColumns() {
  setVisibleColumnKeys(DEFAULT_COLUMNS.map(c => c.key));
  renderColumnFilterOptions();
  renderTable();
}

function hideOptionalColumns() {
  setVisibleColumnKeys(DEFAULT_VISIBLE_COLUMNS);
  renderColumnFilterOptions();
  renderTable();
}

function toggleColumnPanel(event) {
  if (event) event.stopPropagation();
  const panel = document.getElementById("columnFilterPanel");
  if (!panel) return;
  panel.classList.toggle("open");
  renderColumnFilterOptions();
}

document.addEventListener("click", event => {
  const panel = document.getElementById("columnFilterPanel");
  if (!panel) return;
  const wrap = event.target.closest && event.target.closest(".column-filter-wrap");
  if (!wrap) panel.classList.remove("open");
});

const SAVED_VIEWS_KEY = "webexSupervisorSavedViews";
const BUILT_IN_VIEWS = {
  "Default View": DEFAULT_VISIBLE_COLUMNS,
  "Receptionist View": ["email", "organization", "extension", "call", "dnd", "status", "duration", "remote_name", "remote_number", "transfer"],
  "Supervisor View": ["email", "organization", "extension", "status", "duration", "display_name", "remote_name", "remote_number", "transfer", "reset_status"],
  "Troubleshooting View": ["email", "organization", "extension", "status", "duration", "display_name", "webex_state", "event_type", "remote_name", "remote_number"],
  "Minimal View": ["email", "extension", "status", "duration", "call", "transfer"]
};

function getSavedViews() {
  try {
    return JSON.parse(localStorage.getItem(SAVED_VIEWS_KEY) || "{}");
  } catch {
    return {};
  }
}

function setSavedViews(views) {
  localStorage.setItem(SAVED_VIEWS_KEY, JSON.stringify(views));
}

function getCurrentViewSnapshot() {
  return {
    columns: getVisibleColumnKeys(),
    order: getColumnOrder()
  };
}

function renderSavedViewSelect(selectedName = "") {
  const select = document.getElementById("savedViewSelect");
  if (!select) return;
  const savedViews = getSavedViews();
  const builtInOptions = Object.keys(BUILT_IN_VIEWS).map(name => `<option value="builtin:${name}">${name}</option>`).join("");
  const customOptions = Object.keys(savedViews).sort().map(name => `<option value="custom:${name}">${name}</option>`).join("");
  select.innerHTML = `
    <option value="">Saved Views</option>
    <optgroup label="Built-in">${builtInOptions}</optgroup>
    <optgroup label="Custom">${customOptions || '<option disabled>No custom views</option>'}</optgroup>
  `;
  if (selectedName) select.value = selectedName;
}

function applySavedView(value) {
  if (!value) return;
  if (value.startsWith("builtin:")) {
    const name = value.replace("builtin:", "");
    const columns = BUILT_IN_VIEWS[name] || DEFAULT_VISIBLE_COLUMNS;
    setVisibleColumnKeys(columns);
  } else if (value.startsWith("custom:")) {
    const name = value.replace("custom:", "");
    const saved = getSavedViews()[name];
    if (saved) {
      setVisibleColumnKeys(saved.columns || DEFAULT_VISIBLE_COLUMNS);
      if (saved.order) setColumnOrder(saved.order);
    }
  }

  renderColumnFilterOptions();
  renderTable();
}

function saveCurrentView() {
  const name = prompt("Name this view, for example Reception Desk or Troubleshooting:");
  if (!name || !name.trim()) return;
  const cleanName = name.trim();
  const savedViews = getSavedViews();
  savedViews[cleanName] = getCurrentViewSnapshot();
  setSavedViews(savedViews);
  renderSavedViewSelect(`custom:${cleanName}`);
  showTransferStatus(`Saved view: ${cleanName}`, "success");
}

function deleteSavedView() {
  const select = document.getElementById("savedViewSelect");
  if (!select || !select.value.startsWith("custom:")) {
    showTransferStatus("Select a custom saved view before deleting.", "info");
    return;
  }

  const name = select.value.replace("custom:", "");
  const savedViews = getSavedViews();
  delete savedViews[name];
  setSavedViews(savedViews);
  renderSavedViewSelect();
  showTransferStatus(`Deleted saved view: ${name}`, "success");
}

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
function resetColumnOrder() { localStorage.removeItem(STORAGE_KEY); renderTable(); renderColumnFilterOptions(); }
function getOrderedColumns() {
  const map = Object.fromEntries(DEFAULT_COLUMNS.map(c => [c.key, c]));
  const visible = new Set(getVisibleColumnKeys());
  return getColumnOrder().map(k => map[k]).filter(Boolean).filter(c => visible.has(c.key));
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
    showTransferStatus(`DND refresh complete. Updated ${data.updated || 0} user(s). Failed ${data.failed_count || 0}.`, "success");
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
  } catch (err) {
    console.error(err);
    showTransferStatus("Call request failed before it reached the server. Check browser console and Render logs.", "error");
  }
}

function renderResetStatusButton(agent) {
  const disabled = !agent.person_id;
  const title = disabled ? "Reset unavailable: no user ID found" : "Reset this row back to Not On Call";
  const personId = JSON.stringify(agent.person_id || "");
  const label = JSON.stringify(agent.email || agent.display_name || "this user");
  return `<button class="reset-status-btn" ${disabled ? "disabled" : ""} title="${title}" onclick='resetAgentStatus(${personId}, ${label})'>Reset</button>`;
}

async function resetAgentStatus(personId, userLabel) {
  if (!personId) return;

  // Avoid native confirm() because embedded Webex iframes can behave
  // inconsistently with browser dialogs. This action is local-only and safe:
  // it does not change anything in Webex.
  showTransferStatus(`Resetting ${userLabel} to Not On Call...`, "info");

  try {
    const res = await fetch(`/api/agents/${encodeURIComponent(personId)}/reset-status`, {
      method: "POST",
      credentials: "same-origin"
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.success === false) {
      showTransferStatus(data.detail || data.message || "Reset status failed. Check Render logs.", "error");
      return;
    }

    const localAgent = agents.find(a => String(a.person_id || "") === String(personId));
    if (localAgent) {
      localAgent.status = "Not On Call";
      localAgent.webex_state = null;
      localAgent.event_type = null;
      localAgent.call_id = null;
      localAgent.call_session_id = null;
      localAgent.remote_name = null;
      localAgent.remote_number = null;
      localAgent.remote_call_type = null;
      localAgent.call_direction = null;
      localAgent.is_stale = false;
      localAgent.original_status = null;
      localAgent.state_started_at = new Date().toISOString();
      localAgent.updated_at = new Date().toISOString();
    }

    showTransferStatus(`${userLabel} was reset to Not On Call.`, "success");
    renderTable();
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

function formatDuration(totalSeconds) {
  const total = Math.max(0, Number(totalSeconds || 0));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

async function loadCallLogs() {
  try {
    const res = await fetch("/api/call-logs?limit=10", { cache: "no-store" });
    const data = await res.json();
    recentCallLogs = data.call_logs || [];
    renderCallLogs();
  } catch (err) {
    console.error(err);
    document.getElementById("callLogList").innerHTML = `<div class="empty">Could not load completed calls.</div>`;
  }
}

function renderCallLogs() {
  const list = document.getElementById("callLogList");
  if (!recentCallLogs.length) {
    list.innerHTML = `<div class="empty">No completed authenticated calls have been logged yet.</div>`;
    return;
  }

  list.innerHTML = recentCallLogs.map(log => {
    const user = log.email || log.display_name || "Unknown User";
    const remote = log.remote_name || log.remote_number || "Unknown remote";
    const status = log.psa_ticket_status || "not_sent";
    const disabled = status === "sent";
    const statusLabel = status === "sent" ? `PSA Sent${log.psa_ticket_id ? ` #${log.psa_ticket_id}` : ""}` : (status === "failed" ? "PSA Failed" : "PSA Not sent");
    const zohoStatus = log.zoho_status || "not_sent";
    const zohoDisabled = zohoStatus === "sent";
    const zohoLabel = zohoStatus === "sent" ? `Zoho Sent${log.zoho_record_id ? ` #${log.zoho_record_id}` : ""}` : (zohoStatus === "failed" ? "Zoho Failed" : "Zoho Not sent");
    const direction = log.call_direction || "Unknown";
    return `
      <div class="call-log-item">
        <div>${formatEventTime(log.ended_at)}</div>
        <div title="${user}"><strong>${user}</strong><br><span class="activity-meta">${log.org_name || "Unknown Org"}</span></div>
        <div title="${remote}">${remote}<br><span class="direction-badge ${direction}">${direction}</span></div>
        <div><strong>${formatDuration(log.duration_seconds)}</strong></div>
        <div>
          <div class="action-line">
            <div class="psa-status ${status}">${statusLabel}</div>
            <button class="small" ${disabled ? "disabled" : ""} onclick="openSendNoteModal('psa', ${log.id})">Send PSA</button>
            <button class="small detail-button" onclick="openCallDetailModal(${log.id})">Details</button>
          </div>
          ${status === "failed" && log.psa_error ? `<div class="activity-meta" title="${log.psa_error}">${log.psa_error}</div>` : ""}

          <div class="action-line zoho-line">
            <div class="psa-status ${zohoStatus}">${zohoLabel}</div>
            <button class="small zoho-button" ${zohoDisabled ? "disabled" : ""} title="Send to Zoho as Inbound" onclick="openSendNoteModal('zoho', ${log.id}, 'Inbound')">Zoho Inbound</button>
            <button class="small zoho-button outbound" ${zohoDisabled ? "disabled" : ""} title="Send to Zoho as Outbound" onclick="openSendNoteModal('zoho', ${log.id}, 'Outbound')">Zoho Outbound</button>
          </div>
          ${zohoStatus === "failed" && log.zoho_error ? `<div class="activity-meta" title="${log.zoho_error}">${log.zoho_error}</div>` : ""}
        </div>
      </div>
    `;
  }).join("");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function detailRow(label, value) {
  return `<div class="call-detail-label">${escapeHtml(label)}</div><div class="call-detail-value">${escapeHtml(value || "N/A")}</div>`;
}

function openCallDetailModal(callLogId) {
  const log = recentCallLogs.find(item => Number(item.id) === Number(callLogId));
  if (!log) {
    showTransferStatus("Call details could not be found. Refresh the call log and try again.", "error");
    return;
  }

  const direction = log.call_direction || "Unknown";
  const content = document.getElementById("callDetailContent");
  content.innerHTML = `
    <div class="call-detail-grid">
      ${detailRow("User", log.display_name || log.email || log.person_id)}
      ${detailRow("Email", log.email)}
      ${detailRow("Organization", log.org_name || log.org_id)}
      ${detailRow("Remote Party", log.remote_name)}
      ${detailRow("Remote Number", log.remote_number)}
      ${detailRow("Direction", direction)}
      ${detailRow("Start Time", formatFullDateTime(log.started_at))}
      ${detailRow("End Time", formatFullDateTime(log.ended_at))}
      ${detailRow("Duration", formatDuration(log.duration_seconds))}
    </div>

    <div class="call-detail-section-title">Integration Status</div>
    <div class="call-detail-grid">
      ${detailRow("PSA Status", log.psa_ticket_status || "not_sent")}
      ${detailRow("PSA Ticket ID", log.psa_ticket_id)}
      ${detailRow("PSA Error", log.psa_error)}
      ${detailRow("Zoho Status", log.zoho_status || "not_sent")}
      ${detailRow("Zoho Record ID", log.zoho_record_id)}
      ${detailRow("Zoho Error", log.zoho_error)}
      ${detailRow("Call Note", log.call_note)}
      ${detailRow("PSA Note", log.psa_note)}
      ${detailRow("Zoho Note", log.zoho_note)}
    </div>

    <div class="call-detail-section-title">Webex Details</div>
    <div class="call-detail-grid">
      ${detailRow("Call ID", log.call_id)}
      ${detailRow("Session ID", log.call_session_id)}
      ${detailRow("Remote Call Type", log.remote_call_type)}
      ${detailRow("Created", formatFullDateTime(log.created_at))}
      ${detailRow("Updated", formatFullDateTime(log.updated_at))}
    </div>
  `;
  document.getElementById("callDetailModal").style.display = "flex";
}

function closeCallDetailModal() {
  document.getElementById("callDetailModal").style.display = "none";
}

function formatFullDateTime(value) {
  if (!value) return "N/A";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit"
  });
}

let pendingSendAction = null;

function openSendNoteModal(action, callLogId, directionOverride = null) {
  pendingSendAction = { action, callLogId, directionOverride };
  const title = action === "psa" ? "Send to PSA" : `Send to Zoho${directionOverride ? ` as ${directionOverride}` : ""}`;
  document.getElementById("sendNoteTitle").textContent = title;
  document.getElementById("sendNoteText").textContent = "Add an optional note. It will be included with the record you are sending.";
  document.getElementById("sendNoteInput").value = "";
  document.getElementById("sendNoteConfirmButton").onclick = confirmSendWithNote;
  document.getElementById("sendNoteModal").style.display = "flex";
}

function closeSendNoteModal() {
  document.getElementById("sendNoteModal").style.display = "none";
  pendingSendAction = null;
}

async function confirmSendWithNote() {
  if (!pendingSendAction) return;
  const note = document.getElementById("sendNoteInput").value || "";
  const { action, callLogId, directionOverride } = pendingSendAction;
  closeSendNoteModal();

  if (action === "psa") {
    await sendCallLogToPsa(callLogId, note);
  } else {
    await sendCallLogToZoho(callLogId, directionOverride, note);
  }
}

async function sendCallLogToPsa(callLogId, note = "") {
  showTransferStatus("Sending completed call time to Rev.io PSA...", "info");
  try {
    const res = await fetch(`/api/call-logs/${encodeURIComponent(callLogId)}/send-psa`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ note })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.success) {
      const message = data.detail || data.message || "Rev.io ticket creation failed. Check Render logs and Rev.io settings.";
      showTransferStatus(message, "error");
      await loadCallLogs();
      return;
    }
    showTransferStatus(`Rev.io PSA ticket created${data.ticket_id ? `: ${data.ticket_id}` : ""}.`, "success");
    await loadCallLogs();
  } catch (err) {
    console.error(err);
    showTransferStatus("Rev.io PSA request failed before it reached the server.", "error");
  }
}


async function sendCallLogToZoho(callLogId, directionOverride = null, note = "") {
  const directionText = directionOverride ? ` as ${directionOverride}` : "";
  showTransferStatus(`Sending completed call information to Zoho CRM${directionText}...`, "info");
  try {
    const res = await fetch(`/api/call-logs/${encodeURIComponent(callLogId)}/send-zoho`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ ...(directionOverride ? { direction: directionOverride } : {}), note })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.success) {
      const message = data.detail || data.message || "Zoho CRM record creation failed. Check Render logs and Zoho settings.";
      showTransferStatus(message, "error");
      await loadCallLogs();
      return;
    }
    showTransferStatus(`Zoho CRM record created${data.zoho_record_id ? `: ${data.zoho_record_id}` : ""}.`, "success");
    await loadCallLogs();
  } catch (err) {
    console.error(err);
    showTransferStatus("Zoho CRM request failed before it reached the server.", "error");
  }
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
  showTransferStatus("Refreshing extensions from Webex...", "info");
  try {
    const res = await fetch("/api/refresh-extensions", { method: "POST", credentials: "same-origin" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      showTransferStatus(data.detail || data.message || "Extension refresh failed. Check Render logs.", "error");
      return;
    }
    showTransferStatus(`Extension refresh complete. Updated ${data.updated || 0} of ${data.checked || 0} user(s).`, "success");
    await loadAgents();
  } catch (err) {
    console.error(err);
    showTransferStatus("Extension refresh failed before it reached the server.", "error");
  }
}

async function refreshOrgs() {
  showTransferStatus("Refreshing organizations from Webex...", "info");
  try {
    const res = await fetch("/api/refresh-orgs", { method: "POST", credentials: "same-origin" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      showTransferStatus(data.detail || data.message || "Organization refresh failed. Check Render logs.", "error");
      return;
    }
    showTransferStatus(`Organization refresh complete. Updated ${data.updated || 0} of ${data.checked || 0} user(s).`, "success");
    await loadAgents();
  } catch (err) {
    console.error(err);
    showTransferStatus("Organization refresh failed before it reached the server.", "error");
  }
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
renderColumnFilterOptions();
renderSavedViewSelect();
loadAgents();
loadCallLogs();

// Keep the original fast refresh rate, but only update timer text every second.
// This prevents controls like the DND buttons from being destroyed while a user is selecting an option.
setInterval(loadAgents, 3000);
setInterval(loadCallLogs, 10000);
setInterval(updateDurationCells, 1000);
</script>
</body>
</html>
    """)



@app.post("/api/refresh-orgs")
def api_refresh_orgs():
    """
    Clears the in-memory org cache and refreshes org display names for stored agents.
    This works best when WEBEX_ADMIN_TOKEN is set. If no admin token is set,
    the app tries each authenticated user's stored OAuth token.
    """
    ORG_NAME_CACHE.clear()
    updated = 0
    checked = 0
    failed = []

    with db() as conn:
        rows = conn.execute("SELECT person_id, org_id, org_name, access_token, token_expires_at FROM agents WHERE org_id IS NOT NULL").fetchall()

    for row in rows:
        checked += 1
        org_id = row["org_id"]
        token = None
        try:
            if not WEBEX_ADMIN_TOKEN and is_authenticated_agent_row(row):
                token = get_valid_user_access_token(row["person_id"])
        except Exception as exc:
            print(f"Unable to get stored user token for org refresh {row['person_id']}: {exc}")

        resolved = resolve_org_name(org_id, token)

        if resolved and resolved != org_id and resolved != row["org_name"]:
            with db() as conn:
                conn.execute(
                    "UPDATE agents SET org_name = ?, updated_at = ? WHERE person_id = ?",
                    (resolved, now_iso(), row["person_id"]),
                )
            updated += 1
        elif not resolved or resolved == org_id:
            failed.append(row["person_id"])

    return {
        "success": True,
        "message": "organization refresh complete",
        "checked": checked,
        "updated": updated,
        "failed_count": len(failed),
        "failed": failed[:25],
    }


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



@app.get("/api/call-logs")
def api_call_logs(limit: int = 10):
    safe_limit = max(1, min(limit, 50))
    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM call_logs
            ORDER BY ended_at DESC, id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    return {"count": len(rows), "call_logs": [dict(row) for row in rows]}


@app.post("/api/call-logs/{call_log_id}/send-psa")
async def api_send_call_log_to_psa(call_log_id: int, request: Request):
    note = None
    try:
        payload = await request.json()
        if isinstance(payload, dict):
            note = (payload.get("note") or "").strip()
    except Exception:
        note = None

    result = send_revio_ticket_for_call_log(call_log_id, note=note)
    return {"success": True, "call_log_id": call_log_id, **result}


@app.post("/api/call-logs/{call_log_id}/send-zoho")
async def api_send_call_log_to_zoho(call_log_id: int, request: Request):
    direction_override = None
    note = None
    try:
        payload = await request.json()
        if isinstance(payload, dict):
            direction_override = payload.get("direction")
            note = (payload.get("note") or "").strip()
    except Exception:
        direction_override = None
        note = None

    result = send_zoho_record_for_call_log(call_log_id, direction_override=direction_override, note=note)
    return {"success": True, "call_log_id": call_log_id, **result}


@app.get("/api/zoho/debug")
def api_zoho_debug():
    last_call_log = None
    with db() as conn:
        row = conn.execute("""
            SELECT id, ended_at, email, display_name, zoho_status, zoho_record_id, zoho_error, updated_at
            FROM call_logs
            ORDER BY ended_at DESC, id DESC
            LIMIT 1
        """).fetchone()
        if row:
            last_call_log = dict(row)
    return {
        "zoho": get_zoho_debug(),
        "last_call_log": last_call_log,
        "notes": [
            "This endpoint does not expose Zoho access tokens, refresh tokens, client secrets, or passwords.",
            "Zoho CRM Insert Records uses POST {api-domain}/crm/{version}/{module_api_name} with a data array.",
            "Zoho call notes use POST {api-domain}/crm/{version}/{module_api_name}/{record_id}/Notes and require a Notes create scope.",
            "Default target module is Calls. Set ZOHO_CRM_MODULE to use a different module or custom module API name.",
        ],
    }


@app.get("/api/zoho/debug/sample-payload/{call_log_id}")
def api_zoho_debug_sample_payload(call_log_id: int):
    with db() as conn:
        row = conn.execute("SELECT * FROM call_logs WHERE id = ?", (call_log_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Completed call log was not found.")
    return {"url": get_zoho_insert_url(), "payload": build_zoho_call_payload(dict(row))}


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
    checked = 0
    failed = []

    with db() as conn:
        rows = conn.execute("SELECT person_id, access_token, token_expires_at FROM agents").fetchall()

    for row in rows:
        checked += 1
        token = None
        try:
            # Prefer a fresh stored token for authenticated users when no admin token is set.
            if not WEBEX_ADMIN_TOKEN and is_authenticated_agent_row(row):
                token = get_valid_user_access_token(row["person_id"])
        except Exception as exc:
            print(f"Unable to get stored user token for extension refresh {row['person_id']}: {exc}")

        extension = resolve_user_extension(row["person_id"], token)
        if extension:
            with db() as conn:
                conn.execute(
                    "UPDATE agents SET extension = ?, updated_at = ? WHERE person_id = ?",
                    (extension, now_iso(), row["person_id"]),
                )
            updated += 1
        else:
            failed.append(row["person_id"])

    return {
        "success": True,
        "message": "extension refresh complete",
        "checked": checked,
        "updated": updated,
        "failed_count": len(failed),
        "failed": failed[:25],
    }


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
                call_direction = NULL,
                state_started_at = ?,
                updated_at = ?
            WHERE person_id = ?
            """,
            (ts, ts, person_id),
        )

        updated = conn.execute("SELECT * FROM agents WHERE person_id = ?", (person_id,)).fetchone()

    return {"success": True, "message": "agent status reset", "agent": dict(updated) if updated else None}


@app.post("/api/agents/{person_id}/remove")
def api_remove_agent(person_id: str):
    remove_agent_from_dashboard(person_id)
    return {"message": "agent removed from dashboard"}


@app.post("/api/reset")
def api_reset():
    with db() as conn:
        conn.execute("DELETE FROM agents")
        conn.execute("DELETE FROM events")
        conn.execute("DELETE FROM call_logs")
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
        unauth_before = conn.execute("""
            SELECT COUNT(*) AS count FROM agents
            WHERE (access_token IS NULL OR access_token = '')
              AND (refresh_token IS NULL OR refresh_token = '')
              AND (email IS NULL OR email = '' OR email = person_id)
        """).fetchone()["count"]
        cleanup_event_history(conn)
        unauth_deleted = cleanup_unauthenticated_agents(conn)
        after = conn.execute("SELECT COUNT(*) AS count FROM events").fetchone()["count"]

    # VACUUM must run outside the transaction context for SQLite.
    # Render Postgres handles vacuuming automatically, so skip this for PostgreSQL.
    if not USE_POSTGRES:
        with db() as conn:
            conn.execute("VACUUM")

    return {
        "message": "maintenance cleanup complete",
        "events_before": before,
        "events_after": after,
        "max_event_rows": MAX_EVENT_ROWS,
        "unauthenticated_agents_before": unauth_before,
        "unauthenticated_agents_deleted": unauth_deleted,
        "unauthenticated_agent_retention_seconds": UNAUTHENTICATED_AGENT_RETENTION_SECONDS,
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
