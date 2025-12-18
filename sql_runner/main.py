from __future__ import annotations

import os
import json
import time
import uuid
from typing import Optional, Any, Dict

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from bq_tools import plan_and_run_query

app = FastAPI()


class RunSqlRequest(BaseModel):
    sql: str
    max_dry_run_bytes: Optional[int] = None
    maximum_bytes_billed: Optional[int] = None


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


def _log_json(payload: Dict[str, Any]) -> None:
    # Cloud Run は stdout を Cloud Logging に取り込む
    print(json.dumps(payload, ensure_ascii=False))


@app.post("/run-sql")
def run_sql(req: RunSqlRequest, request: Request) -> Dict[str, Any]:
    run_id = uuid.uuid4().hex
    start = time.time()

    data_project = os.getenv("DATA_PROJECT_ID")
    default_project = os.getenv("GOOGLE_CLOUD_PROJECT")
    project_id = data_project or default_project

    if not project_id:
        # 設定ミスも run_id 付きで残す
        _log_json({
            "event": "run_sql",
            "run_id": run_id,
            "status": "CONFIG_ERROR",
            "message": "DATA_PROJECT_ID / GOOGLE_CLOUD_PROJECT not set",
        })
        raise HTTPException(
            status_code=500,
            detail="DATA_PROJECT_ID も GOOGLE_CLOUD_PROJECT も設定されていません。",
        )

    max_dry = req.max_dry_run_bytes or 5 * 1024**2
    max_bill = req.maximum_bytes_billed or 10 * 1024**2

    result = plan_and_run_query(
        sql=req.sql,
        project_id=project_id,
        max_dry_run_bytes=max_dry,
        maximum_bytes_billed=max_bill,
    )

    elapsed_ms = int((time.time() - start) * 1000)

    # 重要なフィールドだけ抽出（sql全文はログに出さないのが無難）
    exec_result = result.get("execute_result") or {}
    _log_json({
        "event": "run_sql",
        "run_id": run_id,
        "status": result.get("status"),
        "elapsed_ms": elapsed_ms,
        "dry_run_bytes": result.get("dry_run_bytes"),
        "job_id": exec_result.get("job_id"),
        "bytes_processed": exec_result.get("bytes_processed"),
        "num_rows": exec_result.get("num_rows"),
        "client_ip": request.client.host if request.client else None,
        "user_agent": request.headers.get("user-agent"),
    })

    # レスポンスにも run_id を入れておくと、ユーザー報告→ログ検索が一発
    result["run_id"] = run_id
    result["elapsed_ms"] = elapsed_ms
    return result
