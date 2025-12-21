from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional

import google.auth
from google.auth.transport.requests import Request
import httpx


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _resolve_vertex_location() -> Optional[str]:
    vertex_location = _env("VERTEX_LOCATION", "")
    if vertex_location:
        return vertex_location
    bq_location = _env("BQ_LOCATION", "")
    if bq_location and bq_location.lower() not in {"us", "eu"}:
        return bq_location
    return None


def _load_system_prompt() -> str:
    prompt_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "prompts",
        "sql_generator_system_prompt.md",
    )
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()


def _extract_json(text: str) -> str:
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()

    json_match = re.search(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", text, re.DOTALL)
    if json_match:
        return json_match.group(0)
    return text


def _extract_sql(text: str) -> str:
    json_text = _extract_json(text)
    try:
        payload = json.loads(json_text)
    except Exception:
        payload = None

    if isinstance(payload, dict) and isinstance(payload.get("sql"), str):
        return payload["sql"].strip()

    sql_block = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if sql_block:
        return sql_block.group(1).strip()

    return text.strip()


def _generate_content(
    *,
    system_prompt: str,
    user_prompt: str,
    project_id: str,
) -> str:
    location = _resolve_vertex_location()
    if not location:
        raise RuntimeError("VERTEX_LOCATION is not set (and BQ_LOCATION is not a region)")

    model = _env("LLM_MODEL", "gemini-2.0-flash-lite")
    temperature = float(_env("LLM_TEMPERATURE", "0.8"))
    max_tokens = int(_env("LLM_MAX_OUTPUT_TOKENS", "1024"))

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    if not credentials.token:
        raise RuntimeError("failed to obtain access token for Vertex AI")

    url = (
        f"https://{location}-aiplatform.googleapis.com/v1/projects/"
        f"{project_id}/locations/{location}/publishers/google/models/"
        f"{model}:generateContent"
    )
    payload = {
        "systemInstruction": {
            "parts": [{"text": system_prompt}],
        },
        "contents": [
            {
                "role": "user",
                "parts": [{"text": user_prompt}],
            }
        ],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_tokens,
        },
    }
    headers = {"Authorization": f"Bearer {credentials.token}"}

    with httpx.Client(timeout=30.0) as client:
        resp = client.post(url, json=payload, headers=headers)
        try:
            resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(f"Vertex AI error: {resp.status_code} {resp.text[:300]}") from exc

    data = resp.json()
    candidates = data.get("candidates") or []
    if not candidates:
        raise RuntimeError("Vertex AI response missing candidates")

    first = candidates[0] or {}
    content = first.get("content") or {}
    parts = content.get("parts") or []
    if not parts:
        raise RuntimeError("Vertex AI response missing content parts")

    text = parts[0].get("text")
    if not text:
        raise RuntimeError("Vertex AI response missing text")

    return text


def generate_sql_from_search(
    query: str,
    search_result: Dict[str, Any],
    *,
    project_id: str,
) -> str:
    system_prompt = _load_system_prompt()
    user_payload = {
        "user_request": query,
        "semantic_search": {
            "top_k": search_result.get("top_k"),
            "items": search_result.get("items", []),
        },
    }
    user_prompt = json.dumps(user_payload, ensure_ascii=False, indent=2)
    response_text = _generate_content(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        project_id=project_id,
    )
    sql = _extract_sql(response_text)
    if not sql:
        raise RuntimeError("LLM returned empty SQL")
    return sql
