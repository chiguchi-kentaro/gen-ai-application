from typing import Literal, TypedDict, Optional
from execute_query_with_max_bytes import ExecuteResult, execute_query_with_max_bytes
from dry_run import dry_run_query


class PlanAndRunResult(TypedDict):
    status: Literal["DRY_RUN_ERROR", "TOO_EXPENSIVE", "EXECUTION_ERROR", "SUCCESS"]
    dry_run_bytes: Optional[int]
    execute_result: Optional[ExecuteResult]
    message: str


def plan_and_run_query(
    sql: str,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
    max_dry_run_bytes: Optional[int] = None,
    maximum_bytes_billed: Optional[int] = None,
) -> PlanAndRunResult:
    """
    1) dry-run でコスト見積もり
    2) max_dry_run_bytes を超えていれば実行しない
    3) 超えていなければ maximum_bytes_billed を付けて本番実行

    という一連の流れを一本化した関数。
    """
    # 1. dry-run
    dry = dry_run_query(
        sql=sql,
        project_id=project_id,
        location=location,
        max_bytes=max_dry_run_bytes,
    )

    if not dry["ok"] and dry["reason"] == "MAX_BYTES_EXCEEDED":
        return PlanAndRunResult(
            status="TOO_EXPENSIVE",
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
            dry_run_bytes=dry["bytes_processed"],
            execute_result=None,
            message=f"dry-run でエラー発生: {dry['error_type']} - {dry['error_message']}",
        )

    # 2. 実行（maximum_bytes_billed 付き）
    exec_result = execute_query_with_max_bytes(
        sql=sql,
        project_id=project_id,
        location=location,
        maximum_bytes_billed=maximum_bytes_billed,
        preview_rows_limit=50,
    )

    if not exec_result["ok"]:
        return PlanAndRunResult(
            status="EXECUTION_ERROR",
            dry_run_bytes=dry["bytes_processed"],
            execute_result=exec_result,
            message=f"実行時にエラー発生: {exec_result['error_type']} - {exec_result['error_message']}",
        )

    return PlanAndRunResult(
        status="SUCCESS",
        dry_run_bytes=dry["bytes_processed"],
        execute_result=exec_result,
        message=(
            f"クエリ実行に成功しました。dry-run 推定 {dry['bytes_processed']} bytes、"
            f"実際の処理 {exec_result['bytes_processed']} bytes, "
            f"行数 {exec_result['num_rows']} 行。"
        ),
    )
