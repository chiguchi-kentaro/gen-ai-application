# main.py (Slack Gateway for Cloud Run)
# - Receives Slack Events API (app_mention) at POST /slack/events
# - Verifies Slack signature (Signing Secret)
# - ACKs within 3 seconds, runs SQL via SQL Runner in BackgroundTasks
# - Replies to Slack thread with result (or error)
#
# Required env vars:
#   SLACK_SIGNING_SECRET   : Slack App "Signing Secret"
#   SLACK_BOT_TOKEN        : Slack Bot token (xoxb-...)
#   SQL_RUNNER_URL         : e.g. https://llm-bq-api-xxxxxx-uc.a.run.app
#
# Optional env vars:
#   SQL_RUNNER_AUDIENCE    : usually same as SQL_RUNNER_URL (base URL)
#     - If omitted, SQL_RUNNER_URL will be used.
#
# Notes:
# - This version DOES NOT use service-account impersonation.
# - If SQL Runner is private (--no-allow-unauthenticated), it must grant roles/run.invoker
#   to THIS gateway's runtime service account.
#
# Run locally:
#   uvicorn main:app --host 0.0.0.0 --port 8080

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time
from typing import Any, Dict

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

app = FastAPI()

logger = logging.getLogger("gateway")
logger.setLevel(logging.INFO)


# ----------------------------
# Env helpers
# ----------------------------
def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


# ----------------------------
# Slack signature verification
# ----------------------------
def _verify_slack_signature(request: Request, body: bytes) -> None:
    secret = _env("SLACK_SIGNING_SECRET", "")
    sig = request.headers.get("X-Slack-Signature")
    ts = request.headers.get("X-Slack-Request-Timestamp")

    logger.info(
        "slack_verify has_secret=%s has_sig=%s has_ts=%s body_len=%d content_type=%s",
        bool(secret),
        bool(sig),
        bool(ts),
        len(body),
        request.headers.get("content-type"),
    )

    if not secret:
        raise HTTPException(status_code=401, detail="SLACK_SIGNING_SECRET empty")
    if not sig or not ts:
        raise HTTPException(status_code=401, detail="Missing Slack signature headers")

    try:
        ts_i = int(ts)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid Slack timestamp")

    now = int(time.time())
    diff = abs(now - ts_i)
    logger.info("slack_verify now=%d ts=%d diff=%d", now, ts_i, diff)
    if diff > 60 * 5:
        raise HTTPException(status_code=401, detail="Stale Slack request")

    basestring = f"v0:{ts}:".encode("utf-8") + body
    digest = hmac.new(secret.encode("utf-8"), basestring, hashlib.sha256).hexdigest()
    expected = f"v0={digest}"

    logger.info("slack_verify sig_prefix=%s expected_prefix=%s", sig[:12], expected[:12])

    if not hmac.compare_digest(expected, sig):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")


# ----------------------------
# Slack message helpers
# ----------------------------
async def _slack_api_post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    token = _env("SLACK_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("SLACK_BOT_TOKEN not set")

    url = f"https://slack.com/api/{method}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    # HTTP error (rare; Slack usually returns 200 with ok:false)
    try:
        r.raise_for_status()
    except Exception:
        logger.error(
            "slack_api_http_error method=%s status=%d body_prefix=%r",
            method,
            r.status_code,
            r.text[:300],
        )
        raise

    # Slack API-level error
    try:
        data = r.json()
    except Exception:
        logger.error(
            "slack_api_non_json method=%s status=%d body_prefix=%r",
            method,
            r.status_code,
            r.text[:300],
        )
        raise

    if not data.get("ok", False):
        logger.error("slack_api_error method=%s payload=%s resp=%s", method, payload, data)
        raise RuntimeError(f"Slack API error: {data.get('error', 'unknown')}")

    return data


async def _post_message(channel: str, text: str, thread_ts: str | None = None) -> None:
    payload: Dict[str, Any] = {"channel": channel, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    await _slack_api_post("chat.postMessage", payload)


async def _post_ephemeral(channel: str, user: str, text: str) -> None:
    payload: Dict[str, Any] = {"channel": channel, "user": user, "text": text}
    await _slack_api_post("chat.postEphemeral", payload)


# ----------------------------
# SQL extraction
# ----------------------------
def _extract_sql_from_app_mention(text: str) -> str:
    # Remove leading "<@UXXXX>" mention token
    t = (text or "").strip()
    t = re.sub(r"^<@[^>]+>\s*", "", t).strip()
    return t


# ----------------------------
# ID token minting (Cloud Run → Cloud Run)
# ----------------------------
def _jwt_payload(token: str) -> Dict[str, Any]:
    # For logging/debug only. Do NOT log the token itself.
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
        raw = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


def _get_id_token_google_auth(audience: str) -> str:
    """
    Returns an ID token for Cloud Run service-to-service auth (NO impersonation).
    Uses runtime ADC (the Cloud Run service account identity).
    """
    import google.oauth2.id_token
    from google.auth.transport.requests import Request as GARequest

    req = GARequest()
    return google.oauth2.id_token.fetch_id_token(req, audience)


# ----------------------------
# SQL Runner call + reply
# ----------------------------
async def _run_sql_and_reply(channel: str, user: str, thread_ts: str, sql: str) -> None:
    sql_runner_url = _env("SQL_RUNNER_URL", "").rstrip("/")
    if not sql_runner_url:
        await _post_ephemeral(channel, user, "SQL_RUNNER_URL not set")
        return

    audience = (_env("SQL_RUNNER_AUDIENCE", "") or sql_runner_url).rstrip("/")
    token = _get_id_token_google_auth(audience=audience)

    # Debug: who is the caller principal? (useful for 403 run.invoker issues)
    p = _jwt_payload(token)
    logger.info(
        "runner_token_subject email=%s sub=%s aud=%s iss=%s",
        p.get("email"),
        p.get("sub"),
        p.get("aud"),
        p.get("iss"),
    )

    url = f"{sql_runner_url}/run-sql"
    payload = {"sql": sql}

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    ct = r.headers.get("content-type", "")
    body_prefix = r.text[:500]

    logger.info(
        "sql_runner_response status=%d content_type=%s body_prefix=%r url=%s",
        r.status_code,
        ct,
        body_prefix[:200],
        url,
    )

    # Cloud Run errors often come back as HTML; do not try to JSON-decode blindly.
    if r.status_code != 200:
        await _post_message(
            channel,
            f"SQL Runner error: status={r.status_code}, content-type={ct}\n```{body_prefix[:200]}```",
            thread_ts=thread_ts,
        )
        return

    if "application/json" not in ct:
        await _post_message(
            channel,
            f"SQL Runner non-JSON response: content-type={ct}\n```{body_prefix[:200]}```",
            thread_ts=thread_ts,
        )
        return

    try:
        data = r.json()
    except Exception as e:
        await _post_message(
            channel,
            f"SQL Runner JSON decode failed: {type(e).__name__}: {e}\n```{body_prefix[:200]}```",
            thread_ts=thread_ts,
        )
        return

    # Pretty-print response for Slack
    await _post_message(
        channel,
        f"run result:\n```{json.dumps(data, ensure_ascii=False, indent=2)}```",
        thread_ts=thread_ts,
    )


# ----------------------------
# Slack Events endpoint
# ----------------------------
@app.post("/slack/events")
async def slack_events(req: Request, bg: BackgroundTasks):
    """
    Slack Events API receiver.
    - Must ACK within 3 seconds; heavy work is background.
    """
    body = await req.body()
    _verify_slack_signature(req, body)

    # Parse JSON safely from raw bytes
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # URL Verification: return challenge
    if payload.get("type") == "url_verification":
        return JSONResponse({"challenge": payload.get("challenge", "")})

    # Slack retries: ACK only (avoid duplicate execution)
    if req.headers.get("x-slack-retry-num"):
        return JSONResponse({"ok": True})

    if payload.get("type") != "event_callback":
        return JSONResponse({"ok": True})

    event = payload.get("event") or {}

    # Loop prevention
    if event.get("bot_id") or event.get("subtype") == "bot_message":
        return JSONResponse({"ok": True})

    if event.get("type") != "app_mention":
        return JSONResponse({"ok": True})

    channel = str(event.get("channel") or "")
    user = str(event.get("user") or "")
    text = str(event.get("text") or "")
    thread_ts = str(event.get("thread_ts") or event.get("ts") or "")

    sql = _extract_sql_from_app_mention(text)
    if not sql:
        bg.add_task(_post_ephemeral, channel, user, "Usage: @bot <SQL>")
        return JSONResponse({"ok": True})

    # Run background job
    bg.add_task(_run_sql_and_reply, channel, user, thread_ts, sql)
    return JSONResponse({"ok": True})


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}
