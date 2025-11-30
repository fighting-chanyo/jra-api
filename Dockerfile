# Playwright公式のPythonイメージを使用（ブラウザ環境込み）
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# 作業ディレクトリを設定
WORKDIR /app

# 先にrequirements.txtをコピーしてインストール（キャッシュ効率化のため）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwrightのブラウザ（Chromium）をインストール
RUN playwright install chromium

# ソースコードをコピー
COPY . .

# ポート8000を開放
EXPOSE 8000

# サーバー起動コマンド
# host 0.0.0.0 は外部アクセスを受け付けるために必須
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

