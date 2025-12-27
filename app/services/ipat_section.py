from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional


_JST = timezone(timedelta(hours=9))


def today_jst() -> date:
    return datetime.now(_JST).date()


def compute_section_start(
    *,
    today: date,
    is_sale_day: Callable[[date], bool],
    lookback_days: int = 90,
    max_section_span_days: int = 10,
) -> Optional[date]:
    """IPATの『節』開始日を推定して返す。

    前提（ユーザー合意）:
    - 地方は無視
    - JRAの発売日=開催日（racesにレコードがある日）として扱う

    仕様:
    - today が発売日でなければ、直近の発売日まで遡ってアンカーにする
    - アンカー日から、前日へ連続する発売日を遡った開始日を節開始日とする

    Returns:
        節開始日(date) or None（lookback内に発売日が見つからない場合）
    """
    if lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")
    if max_section_span_days <= 0:
        raise ValueError("max_section_span_days must be > 0")

    anchor: Optional[date] = None
    for delta in range(0, lookback_days + 1):
        d = today - timedelta(days=delta)
        if is_sale_day(d):
            anchor = d
            break

    if anchor is None:
        return None

    start = anchor
    for _ in range(max_section_span_days):
        prev = start - timedelta(days=1)
        if not is_sale_day(prev):
            break
        start = prev

    return start


@dataclass(frozen=True)
class SectionInfo:
    section_id: str  # YYYYMMDD (section start)
    section_start: date


def compute_current_section_from_races(
    *,
    supabase,
    today: Optional[date] = None,
    lookback_days: int = 90,
    max_section_span_days: int = 10,
) -> Optional[SectionInfo]:
    """racesテーブルを使って『今節』を推定する。

    racesテーブルの `date` 列（ISO YYYY-MM-DD）に開催日が入っている前提。

    Note:
        supabase は `supabase.Client` を想定（型依存は避ける）。
    """
    base = today or today_jst()

    def _exists_race_on(d: date) -> bool:
        # 存在確認（軽量）。limit(1)で十分。
        res = (
            supabase.table("races")
            .select("id")
            .eq("date", d.isoformat())
            .limit(1)
            .execute()
        )
        data = (
            getattr(res, "data", None)
            if hasattr(res, "data")
            else res.get("data")
            if isinstance(res, dict)
            else None
        )
        return isinstance(data, list) and len(data) > 0

    # まず直近の開催日(=発売日)を1クエリで求める（lookback内）
    start_range = (base - timedelta(days=lookback_days)).isoformat()
    anchor_res = (
        supabase.table("races")
        .select("date")
        .gte("date", start_range)
        .lte("date", base.isoformat())
        .order("date", desc=True)
        .limit(1)
        .execute()
    )
    anchor_data = (
        getattr(anchor_res, "data", None)
        if hasattr(anchor_res, "data")
        else anchor_res.get("data")
        if isinstance(anchor_res, dict)
        else None
    )
    if not isinstance(anchor_data, list) or not anchor_data:
        return None

    anchor_str = (anchor_data[0] or {}).get("date")
    if not anchor_str:
        return None
    try:
        anchor = date.fromisoformat(str(anchor_str))
    except Exception:
        return None

    start = anchor
    for _ in range(max_section_span_days):
        prev = start - timedelta(days=1)
        if not _exists_race_on(prev):
            break
        start = prev

    return SectionInfo(section_id=start.strftime("%Y%m%d"), section_start=start)
