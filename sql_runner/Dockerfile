# Dockerfile
FROM python:3.11-slim

# 必要に応じてロケールなど設定
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 依存パッケージのインストール
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# アプリコードをコピー
COPY . .

# Cloud Run は PORT 環境変数を使う（デフォルトは8080）
ENV PORT=8080

# Uvicorn で FastAPI を起動（PORT環境変数を使用）
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}
