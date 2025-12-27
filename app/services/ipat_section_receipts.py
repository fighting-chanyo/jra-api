from __future__ import annotations

from typing import Iterable, Optional


_FW_TO_HW_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")


def normalize_receipt_no(receipt_no: object) -> str:
    """受付番号を正規化（空白除去・全角数字→半角数字）。"""
    if receipt_no is None:
        return ""
    return str(receipt_no).strip().translate(_FW_TO_HW_DIGITS)


def _chunked_list(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("size must be > 0")
    return [items[i : i + size] for i in range(0, len(items), size)]


def get_existing_section_receipts(*, supabase, user_id: str, section_id: str) -> set[str]:
    """今節で既に取り込み済みの受付番号集合を返す（recent経由の履歴のみ）。"""
    res = (
        supabase.table("ipat_section_receipts")
        .select("receipt_no")
        .eq("user_id", user_id)
        .eq("section_id", section_id)
        .execute()
    )
    data = getattr(res, "data", None) if hasattr(res, "data") else res.get("data") if isinstance(res, dict) else None
    if not isinstance(data, list) or not data:
        return set()
    out: set[str] = set()
    for row in data:
        rn = normalize_receipt_no(row.get("receipt_no"))
        if rn:
            out.add(rn)
    return out


def record_section_receipts(
    *,
    supabase,
    user_id: str,
    section_id: str,
    receipt_nos: Iterable[str],
    chunk_size: int = 500,
) -> int:
    """今節×受付番号を記録する（重複はUNIQUE/UPSERTで吸収）。

    Returns:
        送信した件数（=入力のユニーク数）
    """
    normalized = sorted({normalize_receipt_no(r) for r in receipt_nos if normalize_receipt_no(r)})
    if not normalized:
        return 0

    total = 0
    for chunk in _chunked_list(normalized, chunk_size):
        payload = [{"user_id": user_id, "section_id": section_id, "receipt_no": r} for r in chunk]
        supabase.table("ipat_section_receipts").upsert(
            payload,
            on_conflict="user_id,section_id,receipt_no",
        ).execute()
        total += len(chunk)

    return total
