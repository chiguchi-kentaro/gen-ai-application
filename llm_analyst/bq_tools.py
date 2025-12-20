from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Literal, Optional, List, Dict, Any

from typing_extensions import TypedDict  # ★ 重要: typing.TypedDict ではなくこちら
from google.cloud import bigquery
from google.api_core import exceptions as gcloud_exceptions


# ---------------------------
# Env
# ---------------------------

DATA_PROJECT_ID = os.getenv("DATA_PROJECT_ID")
GCP_DEFAULT_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_LOCATION = os.getenv("BQ_LOCATION")  # 例: "asia-northeast1" / "US"

# ---------------------------
# Allowlist (hard-coded for now)
#   Only allow querying tables in:
#     bigquery-public-data.ncaa_basketball
# ---------------------------

ALLOW_PROJECT_ID = "bigquery-public-data"
ALLOW_DATASET_ID = "ncaa_basketball"


def _resolve_project_id(explicit_project_id: Optional[str]) -> Optional[str]:
    if explicit_project_id:
        return explicit_project_id
    if DATA_PROJECT_ID:
        return DATA_PROJECT_ID
    if GCP_DEFAULT_PROJECT:
        return GCP_DEFAULT_PROJECT
    return None


# ---------------------------
# SQL Validator (non-LLM)
# ---------------------------

@dataclass(frozen=True)
class SqlValidationResult:
    ok: bool
    reason: Optional[str] = None
    sanitized_sql: Optional[str] = None


FORBIDDEN_KEYWORDS = {
    # DDL
    "create", "alter", "drop", "truncate", "replace",
    # DML
    "insert", "update", "delete", "merge",
    # BigQuery scripting / dynamic SQL
    "declare", "begin", "end", "execute", "immediate",
    # permissions / exports / procedures（必要なら調整）
    "grant", "revoke", "export", "load", "copy", "call",
}


def _strip_comments(sql: str) -> str:
    """-- と /* */ コメントを除去（文字列リテラル内は保持する簡易実装）"""
    out = []
    i = 0
    n = len(sql)
    in_sq = False  # '
    in_dq = False  # "
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        # quotes toggle
        if not in_dq and ch == "'" and not in_sq:
            in_sq = True
            out.append(ch)
            i += 1
            continue
        elif in_sq and ch == "'":
            if nxt == "'":  # escaped ''
                out.append("''")
                i += 2
                continue
            in_sq = False
            out.append(ch)
            i += 1
            continue

        if not in_sq and ch == '"' and not in_dq:
            in_dq = True
            out.append(ch)
            i += 1
            continue
        elif in_dq and ch == '"':
            in_dq = False
            out.append(ch)
            i += 1
            continue

        # comments (only outside quotes)
        if not in_sq and not in_dq:
            if ch == "-" and nxt == "-":
                i += 2
                while i < n and sql[i] not in ("\n", "\r"):
                    i += 1
                continue
            if ch == "/" and nxt == "*":
                i += 2
                while i + 1 < n and not (sql[i] == "*" and sql[i + 1] == "/"):
                    i += 1
                i += 2
                continue

        out.append(ch)
        i += 1
    return "".join(out)


def _has_semicolon_outside_quotes(sql: str) -> bool:
    in_sq = False
    in_dq = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if not in_dq and ch == "'" and not in_sq:
            in_sq = True
            i += 1
            continue
        if in_sq and ch == "'":
            if nxt == "'":
                i += 2
                continue
            in_sq = False
            i += 1
            continue
        if not in_sq and ch == '"' and not in_dq:
            in_dq = True
            i += 1
            continue
        if in_dq and ch == '"':
            in_dq = False
            i += 1
            continue

        if not in_sq and not in_dq and ch == ";":
            return True
        i += 1
    return False


def _normalize_spaces(sql: str) -> str:
    sql = sql.strip()
    sql = re.sub(r"[ \t]+", " ", sql)
    return sql


def _starts_with_select_or_with(sql: str) -> bool:
    head = sql.lstrip().lower()
    return head.startswith("select") or head.startswith("with")


def _contains_forbidden_keyword(sql: str) -> Optional[str]:
    lowered = sql.lower()
    for kw in sorted(FORBIDDEN_KEYWORDS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(kw)}\b", lowered):
            return kw
    return None


def _ensure_limit(sql: str, default_limit: int) -> str:
    if re.search(r"\blimit\b", sql.lower()):
        return sql
    return f"{sql.rstrip()}\nLIMIT {default_limit}"


def validate_sql(raw_sql: str, *, default_limit: int = 1000) -> SqlValidationResult:
    if not raw_sql or not raw_sql.strip():
        return SqlValidationResult(False, "EMPTY_SQL")

    sql = _strip_comments(raw_sql)
    sql = _normalize_spaces(sql)

    if _has_semicolon_outside_quotes(sql):
        return SqlValidationResult(False, "MULTI_STATEMENT_NOT_ALLOWED")

    if not _starts_with_select_or_with(sql):
        return SqlValidationResult(False, "ONLY_SELECT_ALLOWED")

    bad = _contains_forbidden_keyword(sql)
    if bad:
        return SqlValidationResult(False, f"FORBIDDEN_KEYWORD:{bad}")

    sanitized = _ensure_limit(sql, default_limit=default_limit)
    return SqlValidationResult(True, None, sanitized)


# ---------------------------
# Allowlist validator (dry-run referenced tables)
# ---------------------------

def _validate_referenced_tables_allowlist(
    referenced_tables: Optional[List[Dict[str, str]]],
) -> Optional[str]:
    """
    referenced_tables: [{"projectId": "...", "datasetId": "...", "tableId": "..."} ...]
    戻り値:
      - None: OK
      - "TOO_MANY_REFERENCED_TABLES": 参照が多すぎて安全に判定できないので拒否
      - "<proj>.<dataset>.<table>": allowlist 外の参照があったので拒否
    """
    if referenced_tables is None:
        # 取得できない場合は安全側で拒否したいならここで理由を返す。
        # ただ、通常は None は例外時なので、ここは保守的に拒否。
        return "REFERENCED_TABLES_UNAVAILABLE"

    if len(referenced_tables) == 0:
        # SELECT 1 などテーブル参照なしは許可
        return None

    # referenced_tables は 50 以上だと完全な一覧にならない可能性があるため安全側に倒す
    if len(referenced_tables) >= 50:
        return "TOO_MANY_REFERENCED_TABLES"

    for t in referenced_tables:
        p = t.get("projectId")
        d = t.get("datasetId")
        tb = t.get("tableId")
        if p != ALLOW_PROJECT_ID or d != ALLOW_DATASET_ID:
            return f"{p}.{d}.{tb}"
    return None


# ---------------------------
# Result Types
# ---------------------------

class DryRunResult(TypedDict):
    ok: bool
    bytes_processed: Optional[int]
    referenced_tables: Optional[List[Dict[str, str]]]
    reason: Optional[str]
    error_type: Optional[str]
    error_message: Optional[str]


class ExecuteResult(TypedDict):
    ok: bool
    job_id: Optional[str]
    bytes_processed: Optional[int]
    billing_tier: Optional[int]
    num_rows: Optional[int]
    preview_rows: Optional[List[Dict[str, Any]]]
    error_type: Optional[str]
    error_message: Optional[str]


class PlanAndRunResult(TypedDict):
    status: Literal["INVALID_SQL", "DRY_RUN_ERROR", "TOO_EXPENSIVE", "NOT_ALLOWED", "EXECUTION_ERROR", "SUCCESS"]
    sanitized_sql: Optional[str]
    dry_run_bytes: Optional[int]
    execute_result: Optional[ExecuteResult]
    message: str


# ---------------------------
# BigQuery execution (location is env-only)
# ---------------------------

def dry_run_query(
    sql: str,
    project_id: Optional[str] = None,
    max_bytes: Optional[int] = None,
) -> DryRunResult:
    effective_project_id = _resolve_project_id(project_id)
    client = bigquery.Client(project=effective_project_id) if effective_project_id else bigquery.Client()

    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
    )

    try:
        job = client.query(sql, job_config=job_config, location=BQ_LOCATION)
        bytes_processed = job.total_bytes_processed

        # referenced tables を正規化して dict にする
        refs: List[Dict[str, str]] = []
        for r in (job.referenced_tables or []):
            # TableReference: project / dataset_id / table_id
            refs.append({"projectId": r.project, "datasetId": r.dataset_id, "tableId": r.table_id})

        if max_bytes is not None and bytes_processed is not None and bytes_processed > max_bytes:
            return DryRunResult(
                ok=False,
                bytes_processed=bytes_processed,
                referenced_tables=refs,
                reason="MAX_BYTES_EXCEEDED",
                error_type=None,
                error_message=None,
            )

        return DryRunResult(
            ok=True,
            bytes_processed=bytes_processed,
            referenced_tables=refs,
            reason=None,
            error_type=None,
            error_message=None,
        )

    except gcloud_exceptions.GoogleAPIError as e:
        return DryRunResult(
            ok=False,
            bytes_processed=None,
            referenced_tables=None,
            reason="BQ_ERROR",
            error_type=e.__class__.__name__,
            error_message=str(e),
        )


def execute_query_with_max_bytes(
    sql: str,
    project_id: Optional[str] = None,
    maximum_bytes_billed: Optional[int] = None,
    preview_rows_limit: int = 50,
) -> ExecuteResult:
    effective_project_id = _resolve_project_id(project_id)
    client = bigquery.Client(project=effective_project_id) if effective_project_id else bigquery.Client()

    job_config = bigquery.QueryJobConfig()
    if maximum_bytes_billed is not None:
        job_config.maximum_bytes_billed = maximum_bytes_billed

    try:
        job = client.query(sql, job_config=job_config, location=BQ_LOCATION)
        result_iter = job.result(page_size=preview_rows_limit)

        preview_rows: List[Dict[str, Any]] = []
        for i, row in enumerate(result_iter):
            if i >= preview_rows_limit:
                break
            preview_rows.append(dict(row))

        return ExecuteResult(
            ok=True,
            job_id=job.job_id,
            bytes_processed=job.total_bytes_processed,
            billing_tier=job.billing_tier,
            num_rows=result_iter.total_rows,
            preview_rows=preview_rows,
            error_type=None,
            error_message=None,
        )

    except gcloud_exceptions.GoogleAPIError as e:
        return ExecuteResult(
            ok=False,
            job_id=None,
            bytes_processed=None,
            billing_tier=None,
            num_rows=None,
            preview_rows=None,
            error_type=e.__class__.__name__,
            error_message=str(e),
        )


def plan_and_run_query(
    sql: str,
    project_id: Optional[str] = None,
    max_dry_run_bytes: Optional[int] = None,
    maximum_bytes_billed: Optional[int] = None,
    *,
    default_limit: int = 1000,   # LIMIT 自動付与の既定値
) -> PlanAndRunResult:
    """
    0) SQL バリデーション（非LLM）
    1) dry-run でコスト見積もり
    2) allowlist（参照テーブルが bigquery-public-data.ncaa_basketball のみ）チェック
    3) max_dry_run_bytes 超過なら実行しない
    4) 問題なければ maximum_bytes_billed 付きで本番実行
    """
    if not BQ_LOCATION:
        return PlanAndRunResult(
            status="DRY_RUN_ERROR",
            sanitized_sql=None,
            dry_run_bytes=None,
            execute_result=None,
            message="BQ_LOCATION が設定されていません。（例: asia-northeast1 / US）",
        )

    v = validate_sql(sql, default_limit=default_limit)
    if not v.ok:
        return PlanAndRunResult(
            status="INVALID_SQL",
            sanitized_sql=None,
            dry_run_bytes=None,
            execute_result=None,
            message=f"SQL バリデーションで拒否: {v.reason}",
        )

    sanitized_sql = v.sanitized_sql or sql

    dry = dry_run_query(
        sql=sanitized_sql,
        project_id=project_id,
        max_bytes=max_dry_run_bytes,
    )

    # dry-run が allowlist 以前に落ちてるケース
    if not dry["ok"] and dry["reason"] == "MAX_BYTES_EXCEEDED":
        return PlanAndRunResult(
            status="TOO_EXPENSIVE",
            sanitized_sql=sanitized_sql,
            dry_run_bytes=dry["bytes_processed"],
            execute_result=None,
            message=(
                f"dry-run で推定 {dry['bytes_processed']} bytes でした。"
                f"上限 {max_dry_run_bytes} bytes を超えるため実行しません。"
            ),
        )

    if not dry["ok"]:
        return PlanAndRunResult(
            status="DRY_RUN_ERROR",
            sanitized_sql=sanitized_sql,
            dry_run_bytes=dry["bytes_processed"],
            execute_result=None,
            message=f"dry-run でエラー発生: {dry['error_type']} - {dry['error_message']}",
        )

    # ★ allowlist チェック（dry-run で得た参照テーブルに基づく）
    violation = _validate_referenced_tables_allowlist(dry.get("referenced_tables"))
    if violation is not None:
        if violation == "TOO_MANY_REFERENCED_TABLES":
            detail = "参照テーブル数が多すぎるため、安全に判定できず拒否しました。"
        elif violation == "REFERENCED_TABLES_UNAVAILABLE":
            detail = "参照テーブル情報を取得できないため、安全側で拒否しました。"
        else:
            detail = f"allowlist 外の参照を検出しました: {violation}"

        return PlanAndRunResult(
            status="NOT_ALLOWED",
            sanitized_sql=sanitized_sql,
            dry_run_bytes=dry["bytes_processed"],
            execute_result=None,
            message=(
                "allowlist 違反のため拒否しました。"
                f"許可: {ALLOW_PROJECT_ID}.{ALLOW_DATASET_ID} のみ。{detail}"
            ),
        )

    exec_result = execute_query_with_max_bytes(
        sql=sanitized_sql,
        project_id=project_id,
        maximum_bytes_billed=maximum_bytes_billed,
        preview_rows_limit=50,
    )

    if not exec_result["ok"]:
        return PlanAndRunResult(
            status="EXECUTION_ERROR",
            sanitized_sql=sanitized_sql,
            dry_run_bytes=dry["bytes_processed"],
            execute_result=exec_result,
            message=f"実行時にエラー発生: {exec_result['error_type']} - {exec_result['error_message']}",
        )

    return PlanAndRunResult(
        status="SUCCESS",
        sanitized_sql=sanitized_sql,
        dry_run_bytes=dry["bytes_processed"],
        execute_result=exec_result,
        message=(
            f"クエリ実行に成功しました。dry-run 推定 {dry['bytes_processed']} bytes、"
            f"実際の処理 {exec_result['bytes_processed']} bytes, "
            f"行数 {exec_result['num_rows']} 行。"
        ),
    )
