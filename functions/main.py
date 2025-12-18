import os
from plan_and_run_query import plan_and_run_query

SQL = """
SELECT *
FROM `bigquery-public-data.ncaa_basketball.mbb_historical_teams_seasons`
"""

result = plan_and_run_query(
    sql=SQL,
    project_id=os.getenv("GOOGLE_CLOUD_PROJECT"),
    location="us",
    max_dry_run_bytes=5 * 1024**2,  # dry-run 判定用の上限（例: 5MB）
    maximum_bytes_billed=10 * 1024**2,  # 実行時の絶対上限（例: 10MB）
)

if result["status"] == "SUCCESS":
    print("クエリ実行成功")
elif result["status"] == "DRY_RUN_ERROR":
    print("クエリ実行NG: dry-run エラー")
elif result["status"] == "TOO_EXPENSIVE":
    print("クエリ実行NG: コスト超過")
elif result["status"] == "EXECUTION_ERROR":
    print("クエリ実行NG: 実行エラー")
