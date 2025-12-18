from __future__ import annotations

import os
from typing import Optional, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from bq_tools import plan_and_run_query

app = FastAPI()


class RunSqlRequest(BaseModel):
    sql: str
    location: Optional[str] = None
    project_id: Optional[str] = None
    max_dry_run_bytes: Optional[int] = None
    maximum_bytes_billed: Optional[int] = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run-sql")
def run_sql(req: RunSqlRequest):
    """
    BigQuery のクエリを
      1) dry-run でコスト見積もり
      2) 上限超過なら実行しない
      3) 問題なければ maximum_bytes_billed 付きで実行
    する API。
    """
    project_id = req.project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        raise HTTPException(status_code=500, detail="project_id が指定されていません。")

    max_dry = req.max_dry_run_bytes or 5 * 1024**2  # 5MB
    max_bill = req.maximum_bytes_billed or 10 * 1024**2  # 10MB

    result = plan_and_run_query(
        sql=req.sql,
        project_id=project_id,
        location=req.location,
        max_dry_run_bytes=max_dry,
        maximum_bytes_billed=max_bill,
    )

    if hasattr(result, "model_dump"):        # Pydantic v2 モデル
        return result.model_dump()
    if hasattr(result, "dict"):              # Pydantic v1 モデル
        return result.dict()
    if hasattr(result, "__dict__"):          # 普通のクラス / dataclass
        return result.__dict__
    return result                            # もともと dict ならそのまま
