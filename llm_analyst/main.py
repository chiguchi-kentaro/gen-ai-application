# main.py（Cloud Run 用 Slack Gateway）
# - POST /slack/events で Slack Events API（app_mention）を受信
# - Slack 署名を検証（Signing Secret）
# - 3秒以内にACKし、重い処理は BackgroundTasks で実行
# - 結果（またはエラー）を Slack のスレッドに返信
#
# 必須環境変数:
#   SLACK_SIGNING_SECRET   : Slack App の「Signing Secret」
#   SLACK_BOT_TOKEN        : Slack Bot トークン（xoxb-...）
#
# BigQuery / Vertex AI 関連環境変数:
#   BQ_LOCATION            : 例 "asia-northeast1" / "US"
#   PROJECT_ID             : 任意、GOOGLE_CLOUD_PROJECT を上書き
#   GOOGLE_CLOUD_PROJECT   : PROJECT_ID が空のときのフォールバック
#   VERTEX_LOCATION        : Vertex AI のリージョン（例: asia-northeast1）
#   EMBEDDING_MODEL        : 例 "text-embedding-005"
#   EMBEDDING_META_TABLE   : 例 "kentaro-388002.meta.embedding_meta_data"
#   EMBEDDING_COLUMN       : 例 "embedding"
#   EMBEDDING_TOP_K        : int、デフォルト 8
#
# ローカル起動:
#   uvicorn main:app --host 0.0.0.0 --port 8080

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import time
import uuid
from typing import Any, Dict

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from bq_tools import search_embedding_meta_data
from llm import generate_sql_from_search

app = FastAPI()

logger = logging.getLogger("gateway")
logger.setLevel(logging.INFO)


# ----------------------------
# 環境変数ヘルパー
# ----------------------------
def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)



def _resolve_project_id() -> str | None:
    data_project = _env("PROJECT_ID", "")
    default_project = _env("GOOGLE_CLOUD_PROJECT", "")
    return data_project or default_project or None


def _log_json(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False))


# ----------------------------
# Slack 署名検証
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
# Slack メッセージ送信ヘルパー
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

    # HTTPエラー（稀。Slackは通常200で ok:false を返す）
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

    # Slack API レベルのエラー
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
# クエリ抽出
# ----------------------------
def _extract_query_from_app_mention(text: str) -> str:
    # 先頭の "<@UXXXX>" メンションを除去
    t = (text or "").strip()
    t = re.sub(r"^<@[^>]+>\s*", "", t).strip()
    return t


# ----------------------------
# 関連メタデータ検索 + 返信
# ----------------------------
async def _run_semantic_search_and_generate_sql(
    channel: str,
    user: str,
    thread_ts: str,
    query: str,
) -> None:
    run_id = uuid.uuid4().hex
    start = time.time()

    project_id = _resolve_project_id()
    if not project_id:
        await _post_message(
            channel,
            "PROJECT_ID / GOOGLE_CLOUD_PROJECT not set",
            thread_ts=thread_ts,
        )
        _log_json({
            "event": "semantic_search",
            "run_id": run_id,
            "status": "CONFIG_ERROR",
            "message": "PROJECT_ID / GOOGLE_CLOUD_PROJECT not set",
            "slack_channel": channel,
            "slack_user": user,
        })
        return

    try:
        result = await asyncio.to_thread(
            search_embedding_meta_data,
            text=query,
            project_id=project_id,
        )
    except Exception as e:
        await _post_message(
            channel,
            f"Semantic search error: {type(e).__name__}: {e}",
            thread_ts=thread_ts,
        )
        _log_json({
            "event": "semantic_search",
            "run_id": run_id,
            "status": "UNHANDLED_ERROR",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "slack_channel": channel,
            "slack_user": user,
        })
        return

    try:
        sql = await asyncio.to_thread(
            generate_sql_from_search,
            query,
            result,
            project_id=project_id,
        )
    except Exception as e:
        await _post_message(
            channel,
            f"SQL generation error: {type(e).__name__}: {e}",
            thread_ts=thread_ts,
        )
        _log_json({
            "event": "sql_generation",
            "run_id": run_id,
            "status": "UNHANDLED_ERROR",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "slack_channel": channel,
            "slack_user": user,
        })
        return

    elapsed_ms = int((time.time() - start) * 1000)
    _log_json({
        "event": "sql_generation",
        "run_id": run_id,
        "status": "SUCCESS",
        "elapsed_ms": elapsed_ms,
        "slack_channel": channel,
        "slack_user": user,
    })

    await _post_message(
        channel,
        f"generated SQL:\n```sql\n{sql}\n```",
        thread_ts=thread_ts,
    )


# ----------------------------
# Slack Events エンドポイント
# ----------------------------
@app.post("/slack/events")
async def slack_events(req: Request, bg: BackgroundTasks):
    """
    Slack Events API の受信。
    - 3秒以内にACKし、重い処理はバックグラウンドで実行
    """
    body = await req.body()
    _verify_slack_signature(req, body)

    # raw bytes から安全に JSON をパース
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # URL Verification: challenge を返す
    if payload.get("type") == "url_verification":
        return JSONResponse({"challenge": payload.get("challenge", "")})

    # Slack の再送: ACK のみ返す（重複実行を回避）
    if req.headers.get("x-slack-retry-num"):
        return JSONResponse({"ok": True})

    if payload.get("type") != "event_callback":
        return JSONResponse({"ok": True})

    event = payload.get("event") or {}

    # ループ防止
    if event.get("bot_id") or event.get("subtype") == "bot_message":
        return JSONResponse({"ok": True})

    if event.get("type") != "app_mention":
        return JSONResponse({"ok": True})

    channel = str(event.get("channel") or "")
    user = str(event.get("user") or "")
    text = str(event.get("text") or "")
    thread_ts = str(event.get("thread_ts") or event.get("ts") or "")

    query = _extract_query_from_app_mention(text)
    if not query:
        bg.add_task(_post_ephemeral, channel, user, "Usage: @bot <自然言語クエリ>")
        return JSONResponse({"ok": True})

    await _post_ephemeral(channel, user, "受け付けました。検索を開始します。")

    # バックグラウンドで実行
    bg.add_task(_run_semantic_search_and_generate_sql, channel, user, thread_ts, query)
    return JSONResponse({"ok": True})


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}
