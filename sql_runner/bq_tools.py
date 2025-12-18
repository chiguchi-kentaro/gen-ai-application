from __future__ import annotations

from typing import Literal, Optional, TypedDict, List, Dict, Any
from google.cloud import bigquery
from google.api_core import exceptions as gcloud_exceptions


class DryRunResult(TypedDict):
    ok: bool                     # 実行してよさそうか？（max_bytes を超えていない 等）
    bytes_processed: Optional[int]
    reason: Optional[str]        # "MAX_BYTES_EXCEEDED" などの機械判定理由
    error_type: Optional[str]    # BigQuery 側のエラー種別
    error_message: Optional[str] # エラーメッセージ（ログ用）


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
    status: Literal["DRY_RUN_ERROR", "TOO_EXPENSIVE", "EXECUTION_ERROR", "SUCCESS"]
    dry_run_bytes: Optional[int]
    execute_result: Optional[ExecuteResult]
    message: str


def dry_run_query(
    sql: str,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
    max_bytes: Optional[int] = None,
) -> DryRunResult:
    """
    BigQuery のクエリを dry-run し、推定スキャン量（bytes_processed）を返す関数。

    Args:
        sql: 実行したい SQL 文（SELECT のみを想定）
        project_id: 実行プロジェクト ID（None の場合は環境デフォルト）
        location: データセットのロケーション（例: "asia-northeast1"）
        max_bytes: このバイト数を超えたら ok=False にするための閾値

    Returns:
        DryRunResult:
            ok: True なら「このクエリは実行してよさそう」
            bytes_processed: 推定スキャンバイト数
            reason: NG の理由（例: "MAX_BYTES_EXCEEDED"）
            error_type: 例外クラス名
            error_message: エラーメッセージ
    """
    # クライアント生成（認証は環境変数 GOOGLE_APPLICATION_CREDENTIALS などに依存）
    if project_id:
        client = bigquery.Client(project=project_id)
    else:
        client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,  # コスト見積なのでキャッシュは無効化
    )

    try:
        # location はデータセットと同じリージョンを指定すること
        job = client.query(sql, job_config=job_config, location=location)
        bytes_processed = job.total_bytes_processed

        # max_bytes を超えているかどうかで ok を判断
        if max_bytes is not None and bytes_processed is not None:
            if bytes_processed > max_bytes:
                return DryRunResult(
                    ok=False,
                    bytes_processed=bytes_processed,
                    reason="MAX_BYTES_EXCEEDED",
                    error_type=None,
                    error_message=None,
                )

        # dry-run 自体は成功し、かつ max_bytes もクリア
        return DryRunResult(
            ok=True,
            bytes_processed=bytes_processed,
            reason=None,
            error_type=None,
            error_message=None,
        )

    except gcloud_exceptions.GoogleAPIError as e:
        # 権限不足・構文エラー・テーブル不存在など BigQuery 側エラー
        return DryRunResult(
            ok=False,
            bytes_processed=None,
            reason="BQ_ERROR",
            error_type=e.__class__.__name__,
            error_message=str(e),
        )


def execute_query_with_max_bytes(
    sql: str,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
    maximum_bytes_billed: Optional[int] = None,
    preview_rows_limit: int = 50,
) -> ExecuteResult:
    """
    BigQuery のクエリを実行する関数。
    maximum_bytes_billed を設定して、高額クエリを物理的に止める。

    Args:
        sql: 実行する SQL（SELECT のみを想定）
        project_id: 実行プロジェクト ID
        location: ロケーション（例: "asia-northeast1"）
        maximum_bytes_billed:
            ここで指定したバイト数を超える課金はされない。
            超えた場合、JOB はエラー（Billing tier limit exceeded）になる。
        preview_rows_limit:
            結果のプレビューとして返す行数の上限。

    Returns:
        ExecuteResult:
            ok: True なら実行成功
            job_id: BigQuery ジョブ ID
            bytes_processed: 実際に処理されたバイト数
            billing_tier: 課金ティア
            num_rows: 結果全体の件数
            preview_rows: 上位 preview_rows_limit 行を dict 形式で返す
            error_type, error_message: エラー時の情報
    """
    if project_id:
        client = bigquery.Client(project=project_id)
    else:
        client = bigquery.Client()

    job_config = bigquery.QueryJobConfig()
    if maximum_bytes_billed is not None:
        job_config.maximum_bytes_billed = maximum_bytes_billed

    try:
        job = client.query(sql, job_config=job_config, location=location)

        # 結果を取得（プレビュー用に page_size を制御）
        result_iter = job.result(page_size=preview_rows_limit)

        # メタ情報
        bytes_processed = job.total_bytes_processed
        billing_tier = job.billing_tier
        job_id = job.job_id

        # プレビュー行を dict で返す
        preview_rows: List[Dict[str, Any]] = []
        for i, row in enumerate(result_iter):
            if i >= preview_rows_limit:
                break
            # Row オブジェクト → dict
            preview_rows.append(dict(row))

        # 総行数（result_iter.total_rows は遅延評価されているがここで確定する）
        num_rows = result_iter.total_rows

        return ExecuteResult(
            ok=True,
            job_id=job_id,
            bytes_processed=bytes_processed,
            billing_tier=billing_tier,
            num_rows=num_rows,
            preview_rows=preview_rows,
            error_type=None,
            error_message=None,
        )

    except gcloud_exceptions.GoogleAPIError as e:
        # maximum_bytes_billed 超えや権限不足・構文エラーなど
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
