import hashlib
import json
import logging
import time
from app.schemas import IpatAuth
from app.services.supabase_client import get_supabase_client
from app.constants import RACE_COURSE_MAP
from app.scrapers.jra_scraper import scrape_past_history_csv, scrape_recent_history

logger = logging.getLogger(__name__)


def _chunked(iterable, size: int):
    if size <= 0:
        raise ValueError("size must be > 0")
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def _count_new_receipt_ids(supabase, receipt_unique_ids: list[str]) -> tuple[int, int]:
    """既存の receipt_unique_id を照会し、新規件数と既存件数を返す。

    Returns:
        (new_count, existing_count)
    """
    if not receipt_unique_ids:
        return 0, 0

    existing_ids: set[str] = set()
    # PostgREST のURL長やIN句制限を避けるためチャンクする
    for chunk in _chunked(receipt_unique_ids, 200):
        res = supabase.table("tickets").select("receipt_unique_id").in_("receipt_unique_id", chunk).execute()
        data = getattr(res, "data", None) if hasattr(res, "data") else res.get("data") if isinstance(res, dict) else None
        if not data:
            continue
        for row in data:
            rid = row.get("receipt_unique_id")
            if rid:
                existing_ids.add(rid)

    existing_count = len(existing_ids)
    new_count = max(0, len(set(receipt_unique_ids)) - existing_count)
    return new_count, existing_count


def _build_sync_message(new_count: int) -> str:
    if new_count <= 0:
        return "同期が完了しました。新しいデータは見つかりませんでした。"
    return f"同期が完了しました。{new_count}件の新しいデータが見つかりました。"

def _normalize_date(date_str):
    """日付文字列をYYYYMMDD形式に正規化する"""
    if not date_str:
        return ""
    return str(date_str).replace("/", "").replace("-", "").replace("年", "").replace("月", "").replace("日", "").strip()

def _normalize_horse_numbers(content):
    """馬番リストを正規化（ゼロ埋め）し、可能ならソートする"""
    new_content = content.copy()
    
    def process_list(lst, sort=True):
        if not lst: return []
        # ゼロ埋め
        normalized = []
        for x in lst:
            s = str(x).strip()
            if s.isdigit():
                normalized.append(s.zfill(2))
            else:
                normalized.append(s)
        
        if sort:
            return sorted(normalized)
        return normalized

    # axis: positionsがある場合はソートしない (位置情報との対応を維持するため)
    has_positions = bool(new_content.get("positions"))
    if "axis" in new_content:
        new_content["axis"] = process_list(new_content["axis"], sort=not has_positions)
    
    # partners: 常にソートしてOK（相手馬）
    if "partners" in new_content:
        new_content["partners"] = process_list(new_content["partners"], sort=True)
        
    # selections: 各リストをソートしてOK（BOX, FORMATIONの各要素）
    if "selections" in new_content:
        new_content["selections"] = [process_list(s, sort=True) for s in new_content["selections"]]
        
    return new_content

def _map_ticket_to_db_format(ticket_data, user_id):
    """パース済みデータをDBのticketsテーブルの形式に変換する"""
    raw = ticket_data["raw"]
    parsed = ticket_data["parsed"]

    # 日付の正規化
    normalized_date = _normalize_date(raw['race_date_str'])

    # race_id (YYYYMMDDPPRR) の生成
    place_code = RACE_COURSE_MAP.get(raw["race_place"], "00")
    race_no = raw["race_number_str"].zfill(2)
    race_id = f"{normalized_date}{place_code}{race_no}"

    # コンテンツの正規化（馬番のゼロ埋めとソート）
    normalized_content = _normalize_horse_numbers(parsed["content"])

    # receipt_unique_id (ハッシュ化) の生成
    # 正規化されたコンテンツを使用してハッシュを生成することで、recent/past間の表記揺れを吸収する
    content_str = json.dumps(normalized_content, sort_keys=True)
    
    # 【修正】日付を含めて、日をまたいでもユニークな文字列を生成する
    unique_str = f"{normalized_date}-{raw['receipt_no']}-{raw['line_no']}-{content_str}"
    receipt_unique_id = hashlib.md5(unique_str.encode()).hexdigest()

    # total_points の取得または計算
    total_points = parsed.get("total_points", 0)
    if total_points == 0 and parsed["amount_per_point"] > 0:
        total_points = parsed["total_cost"] // parsed["amount_per_point"]

    source = parsed.get("source", "IPAT_SYNC")
    mode = parsed.get("mode", "REAL")

    # DBのレコードを構成
    return {
        "user_id": user_id,
        "race_id": race_id,
        "bet_type": parsed["bet_type"],
        "buy_type": parsed["buy_type"],
        "content": normalized_content, # DBには正規化されたデータを保存する
        "amount_per_point": parsed["amount_per_point"],
        "total_points": total_points,
        "total_cost": parsed["total_cost"],
        "payout": parsed["payout"],
        "status": parsed["status"],
        "source": source,
        "mode": mode,
        "receipt_unique_id": receipt_unique_id
    }


def sync_and_save_past_history(log_id: str, user_id: str, creds: IpatAuth):
    """バックグラウンドで実行されるメインの処理フロー"""
    supabase = get_supabase_client()
    started_at = time.monotonic()
    logger.info("IPAT past sync started log_id=%s user_id=%s", log_id, user_id)

    try:
        # 1. スクレイピングとパース
        parsed_tickets = scrape_past_history_csv(creds)
        if not parsed_tickets:
            # チケットが0件でも正常終了とする
            supabase.table("sync_logs").update({
                "status": "COMPLETED",
                "message": _build_sync_message(0)
            }).eq("id", log_id).execute()
            logger.info("IPAT past sync completed (no tickets) log_id=%s elapsed=%.1fs", log_id, time.monotonic() - started_at)
            return

        # 2. DB形式への変換
        db_records = [_map_ticket_to_db_format(t, user_id) for t in parsed_tickets]
        
        # 【修正】リスト内での重複排除 (receipt_unique_id)
        unique_records_map = {r["receipt_unique_id"]: r for r in db_records}
        db_records = list(unique_records_map.values())

        receipt_ids = [r["receipt_unique_id"] for r in db_records]
        new_count, existing_count = _count_new_receipt_ids(supabase, receipt_ids)

        # 3. DBへ保存 (Upsert)
        logger.info("Upserting %d tickets (past) log_id=%s", len(db_records), log_id)
        supabase.table("tickets").upsert(db_records, on_conflict="receipt_unique_id").execute()

        # --- 成功時のログ更新（既存の upsert の直後に置き換え） ---
        update_payload = {
            "status": "COMPLETED",
            "message": _build_sync_message(new_count)
        }
        res = supabase.table("sync_logs").update(update_payload).eq("id", log_id).execute()

        # supabase-py の返り値は dict-like (data, error) なので両方チェック
        update_error = getattr(res, "error", None) if hasattr(res, "error") else res.get("error") if isinstance(res, dict) else None
        update_data = getattr(res, "data", None) if hasattr(res, "data") else res.get("data") if isinstance(res, dict) else None

        if update_error:
            logger.warning("Failed to update sync_logs (past) log_id=%s error=%s", log_id, update_error)
        else:
            # data が空リストなら対象行が無かった可能性
            if not update_data:
                logger.warning("sync_logs row not found for update (past). Attempting insert. log_id=%s", log_id)
                # フォールバックで挿入（セキュリティに配慮して ipat_auth 等は含めない）
                insert_payload = {
                    "id": log_id,
                    "status": "COMPLETED",
                    "message": _build_sync_message(new_count)
                }
                ins_res = supabase.table("sync_logs").insert(insert_payload).execute()
                ins_error = getattr(ins_res, "error", None) if hasattr(ins_res, "error") else ins_res.get("error") if isinstance(ins_res, dict) else None
                if ins_error:
                    logger.error("Failed to insert sync_logs fallback record (past) log_id=%s error=%s", log_id, ins_error)
                else:
                    logger.info("Inserted fallback sync_logs record (past) log_id=%s", log_id)
            else:
                logger.info("sync_logs updated successfully (past) log_id=%s", log_id)

        logger.info(
            "IPAT past sync completed log_id=%s fetched_unique=%d new=%d existing=%d elapsed=%.1fs",
            log_id,
            len(db_records),
            new_count,
            existing_count,
            time.monotonic() - started_at,
        )

    except Exception as e:
        # エラーメッセージの翻訳
        error_str = str(e)
        user_friendly_error = error_str
        
        if "Login Failed: Invalid Credentials" in error_str:
            user_friendly_error = "ログインに失敗しました。加入者番号、暗証番号、P-ARS番号を確認してください。"
        elif "Session timed out" in error_str:
            user_friendly_error = "セッションがタイムアウトしました。もう一度お試しください。"
        elif "Login Failed or Menu Changed" in error_str:
            user_friendly_error = "ログイン後の画面遷移に失敗しました。メンテナンス中の可能性があります。"
        
        error_message = f"エラーが発生しました: {user_friendly_error}"
        
        logger.exception("IPAT past sync failed log_id=%s error=%s", log_id, error_message)
        try:
            res = supabase.table("sync_logs").update({
                "status": "ERROR",
                "message": error_message
            }).eq("id", log_id).execute()
            err = getattr(res, "error", None) if hasattr(res, "error") else res.get("error") if isinstance(res, dict) else None
            data = getattr(res, "data", None) if hasattr(res, "data") else res.get("data") if isinstance(res, dict) else None
            if err:
                logger.warning("Failed to update sync_logs with ERROR status (past) log_id=%s err=%s", log_id, err)
                # fallback insert
                try:
                    supabase.table("sync_logs").insert({
                        "id": log_id,
                        "status": "ERROR",
                        "message": error_message
                    }).execute()
                    logger.info("Inserted fallback ERROR record into sync_logs (past) log_id=%s", log_id)
                except Exception as ins_e:
                    # 最終的にDB更新できなければローカルに保存（監査用）
                    fname = f"failed_sync_log_{log_id}.log"
                    with open(fname, "w", encoding="utf-8") as f:
                        f.write(f"Failed to update/insert sync_logs for log_id={log_id}\nError: {error_message}\nDB error: {ins_e}\n")
                    logger.error("Also failed to insert fallback sync_log (past) log_id=%s dumped=%s", log_id, fname)
            elif not data:
                logger.warning("sync_logs update returned no data (past) log_id=%s", log_id)
        except Exception as db_error:
            logger.exception("Additionally failed to update sync_logs (past) log_id=%s", log_id)
            fname = f"failed_sync_log_{log_id}.log"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(f"Additionally failed to update sync_logs for log_id={log_id}\nError: {db_error}\nOriginal error: {error_message}\n")
            logger.error("Wrote debug log to %s (past) log_id=%s", fname, log_id)

def sync_and_save_recent_history(log_id: str, user_id: str, creds: IpatAuth):
    """バックグラウンドで実行される直近履歴同期の処理フロー"""
    supabase = get_supabase_client()
    started_at = time.monotonic()
    logger.info("IPAT recent sync started log_id=%s user_id=%s", log_id, user_id)

    try:
        # 1. スクレイピングとパース
        parsed_tickets = scrape_recent_history(creds)
        
        # 2. DB形式への変換
        db_records = [_map_ticket_to_db_format(t, user_id) for t in parsed_tickets]

        # 【修正】リスト内での重複排除 (receipt_unique_id)
        unique_records_map = {r["receipt_unique_id"]: r for r in db_records}
        db_records = list(unique_records_map.values())

        if not db_records:
            # チケットが0件でも正常終了とする
            update_payload = {
                "status": "COMPLETED",
                "message": _build_sync_message(0)
            }
            supabase.table("sync_logs").update(update_payload).eq("id", log_id).execute()
            logger.info("IPAT recent sync completed (no tickets) log_id=%s elapsed=%.1fs", log_id, time.monotonic() - started_at)
            return

        receipt_ids = [r["receipt_unique_id"] for r in db_records]
        new_count, existing_count = _count_new_receipt_ids(supabase, receipt_ids)

        # 3. DBへ保存 (Upsert)
        logger.info("Upserting %d tickets (recent) log_id=%s", len(db_records), log_id)
        supabase.table("tickets").upsert(db_records, on_conflict="receipt_unique_id").execute()

        # --- 即時判定処理 (結果確定済みのレースがあれば判定) ---
        try:
            race_ids = list(set(r["race_id"] for r in db_records))
            if race_ids:
                from app.services.race_service import RaceService
                race_service = RaceService()
                race_service.judge_existing_races(race_ids)
        except Exception as e:
            logger.warning("Error during immediate judgment (recent) log_id=%s: %s", log_id, e)
            # 判定エラーでも同期自体は成功とみなして続行する

        # --- 成功時のログ更新 ---
        update_payload = {
            "status": "COMPLETED",
            "message": _build_sync_message(new_count)
        }
        res = supabase.table("sync_logs").update(update_payload).eq("id", log_id).execute()
        
        # 成功確認のログ出力
        update_data = getattr(res, "data", None) if hasattr(res, "data") else res.get("data") if isinstance(res, dict) else None
        if not update_data:
            logger.warning("sync_logs row not found for update (recent) log_id=%s", log_id)
        
        logger.info(
            "IPAT recent sync completed log_id=%s fetched_unique=%d new=%d existing=%d elapsed=%.1fs",
            log_id,
            len(db_records),
            new_count,
            existing_count,
            time.monotonic() - started_at,
        )

    except Exception as e:
        # エラーメッセージの翻訳
        error_str = str(e)
        user_friendly_error = error_str
        
        if "Login Failed: Invalid Credentials" in error_str:
            user_friendly_error = "ログインに失敗しました。加入者番号、暗証番号、P-ARS番号を確認してください。"
        elif "Session timed out" in error_str:
            user_friendly_error = "セッションがタイムアウトしました。もう一度お試しください。"
        elif "Login Failed or Menu Changed" in error_str:
            user_friendly_error = "ログイン後の画面遷移に失敗しました。メンテナンス中の可能性があります。"
        elif "JRA IPAT is currently closed" in error_str:
            user_friendly_error = "JRA ネット投票ページは現在クローズしています。"
        
        error_message = f"エラーが発生しました: {user_friendly_error}"
        logger.exception("IPAT recent sync failed log_id=%s error=%s", log_id, error_message)
        
        try:
            res = supabase.table("sync_logs").update({
                "status": "ERROR",
                "message": error_message
            }).eq("id", log_id).execute()
            
            err = getattr(res, "error", None) if hasattr(res, "error") else res.get("error") if isinstance(res, dict) else None
            data = getattr(res, "data", None) if hasattr(res, "data") else res.get("data") if isinstance(res, dict) else None
            
            if err or not data:
                # fallback insert
                supabase.table("sync_logs").insert({
                    "id": log_id,
                    "status": "ERROR",
                    "message": error_message
                }).execute()
        except Exception as db_error:
            logger.exception("Additionally failed to update sync_logs (recent) log_id=%s", log_id)
            fname = f"failed_sync_log_{log_id}.log"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(f"Failed to update sync_logs for log_id={log_id}\nError: {db_error}\nOriginal error: {error_message}\n")
            logger.error("Wrote debug log to %s (recent) log_id=%s", fname, log_id)





