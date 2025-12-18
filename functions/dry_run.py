from __future__ import annotations

from typing import Optional, TypedDict
from google.cloud import bigquery
from google.api_core import exceptions as gcloud_exceptions


class DryRunResult(TypedDict):
    ok: bool                     # 実行してよさそうか？（max_bytes を超えていない 等）
    bytes_processed: Optional[int]
    reason: Optional[str]        # "MAX_BYTES_EXCEEDED" などの機械判定理由
    error_type: Optional[str]    # BigQuery 側のエラー種別
    error_message: Optional[str] # エラーメッセージ（ログ用）


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
