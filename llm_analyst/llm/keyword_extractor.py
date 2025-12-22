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
        "extract_keyword.md",
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


def _generate_content(*, system_prompt: str, user_prompt: str, project_id: str) -> str:
    location = _resolve_vertex_location()
    if not location:
        raise RuntimeError("VERTEX_LOCATION is not set (and BQ_LOCATION is not a region)")

    model = _env("KEYWORD_MODEL", _env("LLM_MODEL", "gemini-2.0-flash-lite"))
    temperature = float(_env("KEYWORD_TEMPERATURE", "0"))
    max_tokens = int(_env("KEYWORD_MAX_OUTPUT_TOKENS", "50"))

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


def _normalize_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value if isinstance(v, (str, int, float))]
    return []


def extract_keywords(user_request: str, *, project_id: str) -> Dict[str, Any]:
    system_prompt = _load_system_prompt()
    user_prompt = user_request.strip()
    response_text = _generate_content(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        project_id=project_id,
    )
    json_text = _extract_json(response_text)
    try:
        payload = json.loads(json_text)
    except Exception:
        payload = {}

    return {
        "metrics": _normalize_list(payload.get("metrics")),
        "dimensions": _normalize_list(payload.get("dimensions")),
        "filters": payload.get("filters") if isinstance(payload.get("filters"), list) else [],
        "raw": payload,
    }
