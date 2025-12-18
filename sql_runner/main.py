from __future__ import annotations

import os
from typing import Optional, Any, Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from bq_tools import plan_and_run_query

app = FastAPI()


class RunSqlRequest(BaseModel):
    sql: str
    # location は受け取らない
    max_dry_run_bytes: Optional[int] = None
    maximum_bytes_billed: Optional[int] = None


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/run-sql")
def run_sql(req: RunSqlRequest) -> Dict[str, Any]:
    """
    BigQuery のクエリを
      1) dry-run でコスト見積もり
      2) 上限超過なら実行しない
      3) 問題なければ maximum_bytes_billed 付きで実行
    する API。
    """
    # どのプロジェクトとしてジョブを投げるかを決定
    data_project = os.getenv("DATA_PROJECT_ID")
    default_project = os.getenv("GOOGLE_CLOUD_PROJECT")
    project_id = data_project or default_project

    if not project_id:
        raise HTTPException(
            status_code=500,
            detail="DATA_PROJECT_ID も GOOGLE_CLOUD_PROJECT も設定されていません。",
        )

    # コストガードレール（デフォルト値）
    max_dry = req.max_dry_run_bytes or 5 * 1024**2        # 5MB
    max_bill = req.maximum_bytes_billed or 10 * 1024**2   # 10MB

    result = plan_and_run_query(
        sql=req.sql,
        project_id=project_id,
        max_dry_run_bytes=max_dry,
        maximum_bytes_billed=max_bill,
    )

    # plan_and_run_query は dict を返すのでそのまま返却
    return result
