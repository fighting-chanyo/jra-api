import hashlib
import json
from app.schemas import IpatAuth
from app.services.supabase_client import get_supabase_client
from app.constants import RACE_COURSE_MAP
from app.scrapers.jra_scraper import scrape_past_history_csv, scrape_recent_history

def _map_ticket_to_db_format(ticket_data, user_id):
    """パース済みデータをDBのticketsテーブルの形式に変換する"""
    raw = ticket_data["raw"]
    parsed = ticket_data["parsed"]

    # race_id (YYYYMMDDPPRR) の生成
    place_code = RACE_COURSE_MAP.get(raw["race_place"], "00")
    race_no = raw["race_number_str"].zfill(2)
    race_id = f"{raw['race_date_str']}{place_code}{race_no}"

    # receipt_unique_id (ハッシュ化) の生成
    content_str = json.dumps(parsed["content"], sort_keys=True)
    # 【修正】日付を含めて、日をまたいでもユニークな文字列を生成する
    unique_str = f"{raw['race_date_str']}-{raw['receipt_no']}-{raw['line_no']}-{content_str}"
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
        "content": parsed["content"],
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
    print(f"BACKGROUND JOB STARTED for log_id: {log_id}")

    try:
        # 1. スクレイピングとパース
        parsed_tickets = scrape_past_history_csv(creds)
        if not parsed_tickets:
            # チケットが0件でも正常終了とする
            supabase.table("sync_logs").update({
                "status": "COMPLETED",
                "message": "同期が完了しました。投票履歴は見つかりませんでした。"
            }).eq("id", log_id).execute()
            print(f"✅ BACKGROUND JOB COMPLETED: No tickets found for log_id: {log_id}")
            return

        # 2. DB形式への変換
        db_records = [_map_ticket_to_db_format(t, user_id) for t in parsed_tickets]
        
        # 【修正】リスト内での重複排除 (receipt_unique_id)
        unique_records_map = {r["receipt_unique_id"]: r for r in db_records}
        db_records = list(unique_records_map.values())

        # 3. DBへ保存 (Upsert)
        print(f"   Upserting {len(db_records)} records to 'tickets' table...")
        supabase.table("tickets").upsert(db_records, on_conflict="receipt_unique_id").execute()

        # --- 成功時のログ更新（既存の upsert の直後に置き換え） ---
        update_payload = {
            "status": "COMPLETED",
            "message": f"同期が完了しました。{len(db_records)} 件の投票履歴を保存しました。"
        }
        res = supabase.table("sync_logs").update(update_payload).eq("id", log_id).execute()

        # supabase-py の返り値は dict-like (data, error) なので両方チェック
        update_error = getattr(res, "error", None) if hasattr(res, "error") else res.get("error") if isinstance(res, dict) else None
        update_data = getattr(res, "data", None) if hasattr(res, "data") else res.get("data") if isinstance(res, dict) else None

        if update_error:
            print(f"⚠️ Failed to update sync_logs (error): {update_error}")
        else:
            # data が空リストなら対象行が無かった可能性
            if not update_data:
                print("⚠️ sync_logs row not found for update. Attempting to insert a new log record.")
                # フォールバックで挿入（セキュリティに配慮して ipat_auth 等は含めない）
                insert_payload = {
                    "id": log_id,
                    "status": "COMPLETED",
                    "message": f"同期が完了しました。{len(db_records)} 件の投票履歴を保存しました。"
                }
                ins_res = supabase.table("sync_logs").insert(insert_payload).execute()
                ins_error = getattr(ins_res, "error", None) if hasattr(ins_res, "error") else ins_res.get("error") if isinstance(ins_res, dict) else None
                if ins_error:
                    print(f"❌ Failed to insert sync_logs fallback record: {ins_error}")
                else:
                    print("✅ Inserted fallback sync_logs record.")
            else:
                print("✅ sync_logs updated successfully.")

        print(f"✅ BACKGROUND JOB COMPLETED for log_id: {log_id}")

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
        
        print(f"❌ BACKGROUND JOB FAILED for log_id: {log_id}. Error: {error_message}")
        try:
            res = supabase.table("sync_logs").update({
                "status": "ERROR",
                "message": error_message
            }).eq("id", log_id).execute()
            err = getattr(res, "error", None) if hasattr(res, "error") else res.get("error") if isinstance(res, dict) else None
            data = getattr(res, "data", None) if hasattr(res, "data") else res.get("data") if isinstance(res, dict) else None
            if err:
                print(f"⚠️ Failed to update sync_logs with ERROR status: {err}")
                # fallback insert
                try:
                    supabase.table("sync_logs").insert({
                        "id": log_id,
                        "status": "ERROR",
                        "message": error_message
                    }).execute()
                    print("✅ Inserted fallback ERROR record into sync_logs.")
                except Exception as ins_e:
                    # 最終的にDB更新できなければローカルに保存（監査用）
                    fname = f"failed_sync_log_{log_id}.log"
                    with open(fname, "w", encoding="utf-8") as f:
                        f.write(f"Failed to update/insert sync_logs for log_id={log_id}\nError: {error_message}\nDB error: {ins_e}\n")
                    print(f"❌ Also failed to insert fallback sync_log; dumped info to {fname}")
            elif not data:
                print("⚠️ sync_logs update returned no data; row might not exist.")
        except Exception as db_error:
            print(f"  Additionally failed to update sync_logs: {db_error}")
            fname = f"failed_sync_log_{log_id}.log"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(f"Additionally failed to update sync_logs for log_id={log_id}\nError: {db_error}\nOriginal error: {error_message}\n")
            print(f"❌ Wrote debug log to {fname}")

def sync_and_save_recent_history(log_id: str, user_id: str, creds: IpatAuth):
    """バックグラウンドで実行される直近履歴同期の処理フロー"""
    supabase = get_supabase_client()
    print(f"BACKGROUND JOB STARTED (RECENT) for log_id: {log_id}")

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
                "message": "同期が完了しました。直近の投票履歴は見つかりませんでした。"
            }
            supabase.table("sync_logs").update(update_payload).eq("id", log_id).execute()
            print(f"✅ BACKGROUND JOB COMPLETED: No tickets found for log_id: {log_id}")
            return

        # 3. DBへ保存 (Upsert)
        print(f"   Upserting {len(db_records)} records to 'tickets' table...")
        supabase.table("tickets").upsert(db_records, on_conflict="receipt_unique_id").execute()

        # --- 即時判定処理 (結果確定済みのレースがあれば判定) ---
        try:
            race_ids = list(set(r["race_id"] for r in db_records))
            if race_ids:
                from app.services.race_service import RaceService
                race_service = RaceService()
                race_service.judge_existing_races(race_ids)
        except Exception as e:
            print(f"⚠️ Error during immediate judgment: {e}")
            # 判定エラーでも同期自体は成功とみなして続行する

        # --- 成功時のログ更新 ---
        update_payload = {
            "status": "COMPLETED",
            "message": f"同期が完了しました。{len(db_records)} 件の投票履歴を保存しました。"
        }
        res = supabase.table("sync_logs").update(update_payload).eq("id", log_id).execute()
        
        # 成功確認のログ出力
        update_data = getattr(res, "data", None) if hasattr(res, "data") else res.get("data") if isinstance(res, dict) else None
        if not update_data:
            print(f"⚠️ sync_logs row not found for update (log_id: {log_id}).")
        
        print(f"✅ BACKGROUND JOB COMPLETED for log_id: {log_id}")

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
        print(f"❌ BACKGROUND JOB FAILED for log_id: {log_id}. Error: {error_message}")
        
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
            print(f"  Additionally failed to update sync_logs: {db_error}")
            fname = f"failed_sync_log_{log_id}.log"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(f"Failed to update sync_logs for log_id={log_id}\nError: {db_error}\nOriginal error: {error_message}\n")
            print(f"❌ Wrote debug log to {fname}")





