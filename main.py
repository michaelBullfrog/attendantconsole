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

# In-memory storage for testing. This resets when Render restarts/redeploys.
USERS: Dict[str, Dict[str, Any]] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def classify_status(event: Dict[str, Any]) -> str:
    """
    Webex Calling webhook mapping:
      data.state = alerting   -> Ringing
      data.state = connected  -> On Call
      webhook event = deleted -> Not On Call
    """
    webhook_event = str(event.get("event", "")).lower()
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    state = str(data.get("state", "")).lower()
    event_type = str(data.get("eventType", "")).lower()

    if webhook_event == "deleted" or event_type in {"ended", "released", "disconnected"}:
        return "Not On Call"
    if state in {"alerting", "ringing"} or event_type in {"received", "offered"}:
        return "Ringing"
    if state in {"connected", "held", "remoteheld", "active", "bridged", "consulting", "conference"} or event_type in {"answered", "connected"}:
        return "On Call"
    return "Unknown"


def extract_remote_party(event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    remote = data.get("remoteParty", {})
    return remote if isinstance(remote, dict) else {}


def extract_user_key(event: Dict[str, Any]) -> str:
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

    payload = {
        "name": "User Webex Calling Status",
        "targetUrl": WEBEX_WEBHOOK_TARGET_URL,
        "resource": "telephony_calls",
        "event": "all",
    }

    response = requests.post(
        "https://webexapis.com/v1/webhooks",
        json=payload,
        headers={"Authorization": f"Bearer {user_access_token}", "Content-Type": "application/json"},
        timeout=20,
    )
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
        :root { --bg:#f4f7fb; --card:#fff; --text:#101828; --muted:#667085; --border:#d0d5dd; --blue:#2563eb; }
        * { box-sizing:border-box; }
        body { margin:0; font-family:Arial,Helvetica,sans-serif; background:var(--bg); color:var(--text); }
        header { background:linear-gradient(135deg,#0f172a,#1e293b); color:white; padding:24px 30px; }
        header h1 { margin:0; font-size:26px; } header p { margin:7px 0 0; color:#cbd5e1; font-size:14px; }
        main { padding:22px; max-width:1450px; margin:0 auto; }
        .toolbar { display:flex; justify-content:space-between; gap:12px; flex-wrap:wrap; margin-bottom:16px; }
        .toolbar-left { display:flex; gap:10px; flex-wrap:wrap; }
        input,select { border:1px solid var(--border); border-radius:10px; padding:10px 12px; min-width:230px; font-size:14px; background:white; }
        button,.button { border:0; border-radius:10px; padding:10px 14px; background:var(--blue); color:white; font-size:14px; cursor:pointer; text-decoration:none; }
        button.secondary { background:#475569; }
        .summary { display:grid; grid-template-columns:repeat(4,minmax(150px,1fr)); gap:12px; margin-bottom:16px; }
        .summary-card { background:white; border:1px solid var(--border); border-radius:14px; padding:14px; box-shadow:0 8px 18px rgba(15,23,42,.06); }
        .summary-card .label { color:var(--muted); font-size:13px; } .summary-card .value { font-size:28px; font-weight:800; margin-top:4px; }
        .table-wrap { background:white; border:1px solid var(--border); border-radius:16px; overflow:hidden; box-shadow:0 8px 18px rgba(15,23,42,.06); }
        table { width:100%; border-collapse:collapse; font-size:14px; }
        thead { background:#f8fafc; }
        th { text-align:left; padding:13px 14px; color:#475569; font-size:12px; text-transform:uppercase; letter-spacing:.04em; border-bottom:1px solid var(--border); white-space:nowrap; }
        td { padding:13px 14px; border-bottom:1px solid #eef2f7; vertical-align:middle; }
        tbody tr:hover { background:#f8fafc; }
        .email { font-weight:700; color:#0f172a; word-break:break-word; }
        .subtext { color:var(--muted); font-size:12px; margin-top:2px; word-break:break-word; }
        .pill { display:inline-flex; border-radius:999px; padding:6px 10px; font-weight:800; font-size:12px; white-space:nowrap; }
        .Ringing { background:#fef3c7; color:#92400e; }
        .OnCall { background:#fee2e2; color:#991b1b; }
        .NotOnCall { background:#dcfce7; color:#166534; }
        .Unknown { background:#e5e7eb; color:#374151; }
        .duration { font-weight:800; color:#0f172a; white-space:nowrap; }
        .empty { padding:34px; text-align:center; color:var(--muted); background:white; }
        .note { margin-top:14px; color:var(--muted); font-size:13px; }
        @media (max-width:900px) { .summary{grid-template-columns:repeat(2,1fr)} .table-wrap{overflow-x:auto} table{min-width:1050px} input,select,button,.button{width:100%} }
    </style>
</head>
<body>
<header><h1>Webex Calling Attendant Console</h1><p>User status table for Ringing, On Call, and Not On Call.</p></header>
<main>
    <div class="toolbar">
        <div class="toolbar-left">
            <input id="search" placeholder="Search email, number, state..." oninput="renderTable()" />
            <select id="stateFilter" onchange="renderTable()">
                <option value="All">All States</option><option value="Ringing">Ringing</option><option value="On Call">On Call</option><option value="Not On Call">Not On Call</option><option value="Unknown">Unknown</option>
            </select>
            <button onclick="loadStatus()">Refresh</button><button class="secondary" onclick="resetStatus()">Reset</button>
        </div>
        <a class="button" href="/oauth/start">Connect Webex User</a>
    </div>
    <section class="summary">
        <div class="summary-card"><div class="label">Total Users</div><div class="value" id="totalCount">0</div></div>
        <div class="summary-card"><div class="label">Ringing</div><div class="value" id="ringingCount">0</div></div>
        <div class="summary-card"><div class="label">On Call</div><div class="value" id="onCallCount">0</div></div>
        <div class="summary-card"><div class="label">Not On Call</div><div class="value" id="notOnCallCount">0</div></div>
    </section>
    <div class="table-wrap"><table><thead><tr><th>User Email</th><th>State</th><th>Time in State</th><th>Webex State</th><th>Event Type</th><th>Remote Party</th><th>Remote Number</th><th>Last Updated</th></tr></thead><tbody id="statusBody"><tr><td colspan="8" class="empty">Loading...</td></tr></tbody></table></div>
    <div class="note">Auto-refreshes every 3 seconds. Duration refreshes every second. This test version uses memory only.</div>
</main>
<script>
let users=[];
function cssStatus(s){ if(s==='On Call') return 'OnCall'; if(s==='Not On Call') return 'NotOnCall'; if(s==='Ringing') return 'Ringing'; return 'Unknown'; }
function formatDate(v){ if(!v) return 'N/A'; try{return new Date(v).toLocaleString();}catch{return v;} }
function durationSince(v){ if(!v) return 'N/A'; const start=new Date(v).getTime(); if(Number.isNaN(start)) return 'N/A'; const d=Math.max(0,Math.floor((Date.now()-start)/1000)); const h=Math.floor(d/3600), m=Math.floor((d%3600)/60), s=d%60; if(h>0) return `${h}h ${m}m ${s}s`; if(m>0) return `${m}m ${s}s`; return `${s}s`; }
async function loadStatus(){ try{ const r=await fetch('/api/status',{cache:'no-store'}); const data=await r.json(); users=data.users||[]; renderTable(); }catch(e){ console.error(e); document.getElementById('statusBody').innerHTML='<tr><td colspan="8" class="empty">Failed to load /api/status. Check Render logs.</td></tr>'; } }
function updateSummary(){ document.getElementById('totalCount').textContent=users.length; document.getElementById('ringingCount').textContent=users.filter(u=>u.status==='Ringing').length; document.getElementById('onCallCount').textContent=users.filter(u=>u.status==='On Call').length; document.getElementById('notOnCallCount').textContent=users.filter(u=>u.status==='Not On Call').length; }
function renderTable(){ updateSummary(); const search=document.getElementById('search').value.toLowerCase().trim(); const filter=document.getElementById('stateFilter').value; const filtered=users.filter(u=>(filter==='All'||u.status===filter)&&JSON.stringify(u).toLowerCase().includes(search)); const body=document.getElementById('statusBody'); if(!filtered.length){ body.innerHTML='<tr><td colspan="8" class="empty">No users to display yet. Click <strong>Connect Webex User</strong>, authorize a Webex Calling user, then make or receive a test call.</td></tr>'; return; } body.innerHTML=filtered.map(u=>`<tr><td><div class="email">${u.email||u.displayName||u.userKey||'Unknown User'}</div><div class="subtext">${u.personId||''}</div></td><td><span class="pill ${cssStatus(u.status)}">${u.status||'Unknown'}</span></td><td><span class="duration">${durationSince(u.stateStartedAt||u.updatedAt)}</span></td><td>${u.webexState||'N/A'}</td><td>${u.eventType||'N/A'}</td><td>${u.remoteName||'N/A'}</td><td>${u.remoteNumber||'N/A'}</td><td><div>${formatDate(u.updatedAt)}</div><div class="subtext">${u.callSessionId||''}</div></td></tr>`).join(''); }
async function resetStatus(){ await fetch('/api/status/reset',{method:'POST'}); await loadStatus(); }
loadStatus(); setInterval(loadStatus,3000); setInterval(renderTable,1000);
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
    priority = {"Ringing": 0, "On Call": 1, "Unknown": 2, "Not On Call": 3}
    output.sort(key=lambda u: (priority.get(u.get("status"), 9), u.get("email") or u.get("displayName") or ""))
    return {"count": len(output), "users": output}


@app.get("/api/status/raw")
def api_status_raw():
    return USERS


@app.post("/api/status/reset")
def api_status_reset():
    USERS.clear()
    return {"message": "status reset"}


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

    access_token = token_response.json().get("access_token")
    me = get_me(access_token)
    webhook = create_call_status_webhook(access_token)

    if me:
        emails = me.get("emails") or []
        email = emails[0] if emails else None
        person_id = me.get("id")
        user_key = person_id or email or webhook.get("id") or "unknown-user"
        t = now_iso()
        USERS[user_key] = {
            "userKey": user_key,
            "personId": person_id,
            "displayName": me.get("displayName") or email or user_key,
            "email": email,
            "status": "Not On Call",
            "stateStartedAt": t,
            "updatedAt": t,
            "webexState": None,
            "eventType": "connected",
            "callId": None,
            "callSessionId": None,
            "remoteName": None,
            "remoteNumber": None,
            "remoteCallType": None,
            "webhookId": webhook.get("id"),
        }

    return HTMLResponse(f"""
        <html><head><title>Webex User Connected</title><meta http-equiv="refresh" content="2; url=/" />
        <style>body{{font-family:Arial,sans-serif;background:#f4f7fb;padding:40px}}.card{{background:white;padding:24px;border-radius:14px;max-width:700px;box-shadow:0 8px 18px rgba(15,23,42,.08)}}a{{color:#2563eb}}</style></head>
        <body><div class="card"><h2>Webex user connected</h2><p>The telephony_calls webhook was created successfully.</p><p><strong>Webhook ID:</strong> {webhook.get('id')}</p><p>Redirecting to the attendant console...</p><p><a href="/">Go now</a></p></div></body></html>
    """)


@app.post("/webex/calling-events")
async def calling_events(request: Request):
    event = await request.json()
    print("Received Webex Calling event:")
    print(json.dumps(event, indent=2))

    data = event.get("data", {}) if isinstance(event.get("data"), dict) else {}
    remote = extract_remote_party(event)
    user_key = extract_user_key(event)
    new_status = classify_status(event)
    existing = USERS.get(user_key, {})

    old_status = existing.get("status")
    state_started_at = now_iso() if old_status != new_status else existing.get("stateStartedAt") or now_iso()

    call_id = data.get("callId")
    call_session_id = data.get("callSessionId")
    if new_status == "Not On Call":
        call_id = None
        call_session_id = None
        remote_name = None
        remote_number = None
        remote_call_type = None
    else:
        remote_name = remote.get("name")
        remote_number = remote.get("number")
        remote_call_type = remote.get("callType")

    USERS[user_key] = {
        "userKey": user_key,
        "personId": existing.get("personId") or event.get("actorId") or event.get("createdBy"),
        "displayName": existing.get("displayName") or existing.get("email") or user_key,
        "email": existing.get("email") or user_key,
        "status": new_status,
        "stateStartedAt": state_started_at,
        "updatedAt": now_iso(),
        "webexState": data.get("state"),
        "eventType": data.get("eventType") or event.get("event"),
        "callId": call_id,
        "callSessionId": call_session_id,
        "remoteName": remote_name,
        "remoteNumber": remote_number,
        "remoteCallType": remote_call_type,
        "webhookId": event.get("id") or existing.get("webhookId"),
    }
    return {"status": "received", "user": USERS[user_key]}
