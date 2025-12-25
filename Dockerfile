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

# ポート8080を開放 (Cloud Runのデフォルト)
EXPOSE 8080

# サーバー起動コマンド
# appフォルダの中にあるmain.pyを呼び出すため app.main:app に変更
# ポートを8080に変更
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

