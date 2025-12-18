from __future__ import annotations

from typing import Optional, TypedDict, List, Dict, Any
from google.cloud import bigquery
from google.api_core import exceptions as gcloud_exceptions


class ExecuteResult(TypedDict):
    ok: bool
    job_id: Optional[str]
    bytes_processed: Optional[int]
    billing_tier: Optional[int]
    num_rows: Optional[int]
    preview_rows: Optional[List[Dict[str, Any]]]
    error_type: Optional[str]
    error_message: Optional[str]


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
