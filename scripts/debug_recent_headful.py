import os
import logging
import sys
from pathlib import Path


# Allow running this script from anywhere (so `import app` works)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.schemas import IpatAuth
from app.scrapers.jra_scraper import scrape_recent_history


logging.basicConfig(level=logging.INFO)


def _require(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v


if __name__ == "__main__":
    # 必須: 認証情報は環境変数から
    creds = IpatAuth(
        inet_id=_require("IPAT_INET_ID"),
        subscriber_number=_require("IPAT_SUBSCRIBER_NUMBER"),
        password=_require("IPAT_PASSWORD"),
        pars_number=_require("IPAT_PARS_NUMBER"),
    )

    # デバッグ推奨デフォルト（必要なら上書きOK）
    os.environ.setdefault("HEADLESS", "false")
    os.environ.setdefault("PLAYWRIGHT_SLOW_MO_MS", "250")
    os.environ.setdefault("IPAT_TRACE", "false")
    os.environ.setdefault("IPAT_TRACE_PATH", "/tmp/ipat_recent_trace.zip")
    # デフォルトは安全側（HTML/PNGの保存は任意でON）
    os.environ.setdefault("SAVE_DEBUG_ARTIFACTS", "false")
    # 例: step1/step2/history/all
    os.environ.setdefault("IPAT_DEBUG_PAUSE_AT", "")

    result = scrape_recent_history(creds)
    print(f"tickets={len(result)}")
