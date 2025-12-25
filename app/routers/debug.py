from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import APIRouter


router = APIRouter()


def _now_jst_iso() -> str:
    jst = timezone(timedelta(hours=9))
    return datetime.now(tz=jst).isoformat()


def _new_session() -> requests.Session:
    # Cloud Run等でHTTP(S)_PROXY環境変数の影響を避ける
    s = requests.Session()
    s.trust_env = False
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Connection": "close",
        }
    )
    return s


def _probe(session: requests.Session, url: str, timeout: tuple[float, float] = (5.0, 10.0)) -> Dict[str, Any]:
    started = datetime.now()
    try:
        resp = session.get(url, timeout=timeout)
        elapsed_ms = int((datetime.now() - started).total_seconds() * 1000)
        return {
            "url": url,
            "ok": resp.ok,
            "status_code": resp.status_code,
            "elapsed_ms": elapsed_ms,
        }
    except requests.exceptions.SSLError as e:
        elapsed_ms = int((datetime.now() - started).total_seconds() * 1000)
        return {
            "url": url,
            "ok": False,
            "status_code": None,
            "elapsed_ms": elapsed_ms,
            "error_type": "SSLError",
            "error": str(e),
        }
    except requests.exceptions.RequestException as e:
        elapsed_ms = int((datetime.now() - started).total_seconds() * 1000)
        return {
            "url": url,
            "ok": False,
            "status_code": None,
            "elapsed_ms": elapsed_ms,
            "error_type": type(e).__name__,
            "error": str(e),
        }


@router.get("/debug/egress")
def debug_egress() -> Dict[str, Any]:
    """Cloud Runからの外向き疎通を切り分けるための診断用エンドポイント。

    - SSRFを避けるため、チェック対象URLは固定
    - public_ip が取れて、google がOKなのに netkeiba だけSSLで落ちる…ならブロック/制限の可能性が高い
    """

    session = _new_session()

    checks: List[Dict[str, Any]] = []

    # 外向きIP確認（JSONではなくプレーンテキスト）
    ip_check = _probe(session, "https://api.ipify.org", timeout=(3.0, 5.0))
    public_ip: Optional[str] = None
    if ip_check.get("ok"):
        try:
            # もう一度本文を取りたいので、軽く再取得
            resp = session.get("https://api.ipify.org", timeout=(3.0, 5.0))
            if resp.ok:
                public_ip = resp.text.strip()
        except Exception:
            public_ip = None
    checks.append(ip_check)

    # ベンチマーク（一般サイト）
    checks.append(_probe(session, "https://www.google.com", timeout=(3.0, 8.0)))

    # netkeiba（カレンダー/レース一覧）
    jst = timezone(timedelta(hours=9))
    now = datetime.now(tz=jst)
    year = now.year
    month = now.month
    checks.append(
        _probe(
            session,
            f"https://race.netkeiba.com/top/calendar.html?year={year}&month={month}",
            timeout=(5.0, 15.0),
        )
    )
    checks.append(
        _probe(
            session,
            "https://race.netkeiba.com/top/race_list_sub.html?kaisai_date=20250125",
            timeout=(5.0, 15.0),
        )
    )

    return {
        "timestamp_jst": _now_jst_iso(),
        "public_ip": public_ip,
        "checks": checks,
    }
