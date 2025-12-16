import os
import hashlib
import json
from fastapi import HTTPException
from playwright.sync_api import sync_playwright
from app.schemas import IpatAuth
from app.services.parsers import parse_jra_csv
from app.services.supabase_client import get_supabase_client
from app.constants import RACE_COURSE_MAP

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

    # total_points の計算 (0除算を回避)
    total_points = 0
    if parsed["amount_per_point"] > 0:
        total_points = parsed["total_cost"] // parsed["amount_per_point"]

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
        "source": "IPAT_SYNC",
        "receipt_unique_id": receipt_unique_id
    }


def sync_and_save_past_history(log_id: str, user_id: str, creds: IpatAuth):
    """バックグラウンドで実行されるメインの処理フロー"""
    supabase = get_supabase_client()
    print(f"BACKGROUND JOB STARTED for log_id: {log_id}")

    try:
        # 1. スクレイピングとパース
        parsed_tickets = _scrape_past_history_csv(creds)
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
        # 1. スクレイピングとパース (未実装のためプレースホルダー)
        # parsed_tickets = _scrape_recent_history(creds)
        parsed_tickets = [] # 仮
        
        print("ℹ️ Recent history scraping is currently a placeholder.")

        if not parsed_tickets:
            # チケットが0件でも正常終了とする
            supabase.table("sync_logs").update({
                "status": "COMPLETED",
                "message": "同期が完了しました。直近の投票履歴は見つかりませんでした。"
            }).eq("id", log_id).execute()
            print(f"✅ BACKGROUND JOB COMPLETED: No tickets found for log_id: {log_id}")
            return

        # 以下、実装時はDB保存ロジックを追加

    except Exception as e:
        error_message = f"エラーが発生しました: {str(e)}"
        print(f"❌ BACKGROUND JOB FAILED for log_id: {log_id}. Error: {error_message}")
        try:
            supabase.table("sync_logs").update({
                "status": "ERROR",
                "message": error_message
            }).eq("id", log_id).execute()
        except Exception as db_error:
            print(f"  Additionally failed to update sync_logs: {db_error}")

def _scrape_past_history_csv(creds: IpatAuth):
    """PlaywrightによるスクレイピングとCSVパース処理を担う (旧sync_past_history)"""
    print("🚀 Accessing JRA Vote Inquiry (PC/CSV Mode)...")
    all_parsed_data = []
    
    with sync_playwright() as p:
        is_headless = os.getenv("HEADLESS", "true").lower() != "false"
        browser = p.chromium.launch(headless=is_headless)
        # User-Agentを設定して、一般的なブラウザからのアクセスに見せかける
        context = browser.new_context(
            accept_downloads=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        context.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,css}", lambda route: route.abort())
        page = context.new_page()
        page.on("dialog", lambda dialog: dialog.accept())
        
        print("👉 Logging in to PC site...")
        page.goto("https://www.nvinq.jra.go.jp/jra/")
        
        # デバッグ用：ログインページ保存
        with open("debug_login_page.html", "w", encoding="utf-8") as f: f.write(page.content())

        # 入力値をクリーニング（前後の空白削除）
        s_no = creds.subscriber_number.strip()
        pwd = creds.password.strip()
        pars = creds.pars_number.strip()

        page.wait_for_selector("#UID")
        page.locator("#UID").fill(s_no)
        page.wait_for_timeout(500)
        # ユーザー指摘により入れ替え: 暗証番号欄にP-ARS、P-ARS欄に暗証番号を入力
        page.locator("#PWD").fill(pars)
        page.wait_for_timeout(500)
        page.locator("#PARS").fill(pwd)
        page.wait_for_timeout(500)
        page.locator("input[type='submit'][value='ログイン']").click()
        page.wait_for_load_state("networkidle")

        # エラーメッセージチェック
        if page.locator("text=加入者番号・暗証番号・P-ARS番号に誤りがあります").is_visible():
             with open("debug_login_failed.html", "w", encoding="utf-8") as f: f.write(page.content())
             raise Exception("Login Failed: Invalid Credentials (加入者番号・暗証番号・P-ARS番号に誤りがあります)")

        print("👉 Navigating to Vote Inquiry (JRAWeb320)...")
        menu_btn = page.locator("tr:has-text('投票内容照会') input[type='submit']").first
        if not menu_btn.is_visible():
            menu_btn = page.locator("input[value='選択']").first
        if not menu_btn.is_visible():
            with open("debug_login_failed.html", "w", encoding="utf-8") as f: f.write(page.content())
            raise Exception("Login Failed or Menu Changed. See debug_login_failed.html")
        menu_btn.click()
        page.wait_for_load_state("networkidle")

        print("👉 Navigating to Receipt Number List (JRAWeb020)...")
        accept_link = page.locator("a.toAcceptnoNum")
        if accept_link.is_visible():
            accept_link.click()
        else:
            page.evaluate("document.forms['Go020'].submit()")
        
        # --- ここから修正 ---
        # ページ遷移を待機し、まずセッションエラーの可能性をチェックする
        try:
            # 「日付選択」ページ、または「セッション切れ」ページのどちらかの読み込みが完了するのを待つ
            page.wait_for_load_state("domcontentloaded", timeout=15000)

            # セッション切れ画面の特有のテキストが存在するかどうかで判定
            if page.locator("text=ログインが無効となったか").is_visible():
                raise Exception("Session timed out or became invalid. Please try again.")

        except Exception as e:
            # is_visible()のタイムアウトや、独自にraiseした例外を捕捉
            error_message = str(e) if str(e) else "Failed to determine page state after navigation."
            # デバッグ用に最終的な画面を保存
            with open("debug_navigation_error.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            raise Exception(error_message)

        print("👉 Checking Date List...")
        date_buttons = page.locator("input[value='選択']")
        date_count = date_buttons.count()
        print(f"👀 Found {date_count} date buttons.")

        if date_count == 0:
            # エラーチェックは通過したがボタンがない場合
            print("⚠️ No dates found. Maybe no betting history.")
            return []
        # --- ここまで修正 ---

        for i in range(date_count):
            page.locator("input[value='選択']").nth(i).click()
            page.wait_for_load_state("networkidle")
            csv_btn = page.locator("form[action*='JRACSVDownload'] input[name='normal']")
            if csv_btn.is_visible():
                with page.expect_download() as download_info:
                    csv_btn.click()
                download = download_info.value
                csv_path = f"temp_history_{i}.csv"
                download.save_as(csv_path)
                parsed = parse_jra_csv(csv_path)
                all_parsed_data.extend(parsed)
                os.remove(csv_path)
            
            back_btn = page.locator("input[value*='日付選択']").first
            if back_btn.is_visible():
                back_btn.click()
            else:
                page.go_back()
            page.wait_for_load_state("networkidle")

        browser.close()

    return all_parsed_data