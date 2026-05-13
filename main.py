import os
import requests
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

WEBEX_CLIENT_ID = os.getenv("WEBEX_CLIENT_ID")
WEBEX_CLIENT_SECRET = os.getenv("WEBEX_CLIENT_SECRET")
WEBEX_REDIRECT_URI = os.getenv("WEBEX_REDIRECT_URI")

SCOPES = "spark:calls_read spark:webhooks_write spark:webhooks_read spark:people_read"


@app.get("/")
def home():
    return {"message": "Webex Calling Status app is running"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/oauth/start")
def oauth_start():
    if not WEBEX_CLIENT_ID or not WEBEX_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing Webex OAuth environment variables")

    auth_url = (
        "https://webexapis.com/v1/authorize"
        f"?client_id={WEBEX_CLIENT_ID}"
        "&response_type=code"
        f"&redirect_uri={WEBEX_REDIRECT_URI}"
        f"&scope={SCOPES.replace(' ', '%20')}"
        "&state=webex-calling-status"
    )

    return {"connect_url": auth_url}


@app.get("/oauth/callback")
def oauth_callback(request: Request):
    code = request.query_params.get("code")

    if not code:
        raise HTTPException(status_code=400, detail="Missing authorization code")

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

    webhook = create_call_status_webhook(access_token)

    return {
        "message": "Webex authorization successful. Calling webhook created.",
        "webhook_id": webhook.get("id"),
        "expires_in": tokens.get("expires_in")
    }


def create_call_status_webhook(user_access_token: str):
    webhook_url = "https://webexapis.com/v1/webhooks"

    target_url = os.getenv("WEBEX_WEBHOOK_TARGET_URL")

    if not target_url:
        raise HTTPException(status_code=500, detail="Missing WEBEX_WEBHOOK_TARGET_URL")

    payload = {
        "name": "User Webex Calling Status",
        "targetUrl": target_url,
        "resource": "telephony_calls",
        "event": "all"
    }

    headers = {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(webhook_url, json=payload, headers=headers, timeout=20)

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


@app.post("/webex/calling-events")
async def calling_events(request: Request):
    event = await request.json()

    print("Received Webex Calling event:")
    print(event)

    return {"status": "received"}