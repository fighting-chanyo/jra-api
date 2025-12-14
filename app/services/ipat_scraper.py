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
                "message": "Synced 0 tickets. No betting data found."
            }).eq("id", log_id).execute()
            print(f"✅ BACKGROUND JOB COMPLETED: No tickets found for log_id: {log_id}")
            return

        # 2. DB形式への変換
        db_records = [_map_ticket_to_db_format(t, user_id) for t in parsed_tickets]
        
        # 3. DBへ保存 (Upsert)
        print(f"   Upserting {len(db_records)} records to 'tickets' table...")
        supabase.table("tickets").upsert(db_records, on_conflict="receipt_unique_id").execute()

        # 4. ログを成功ステータスに更新
        supabase.table("sync_logs").update({
            "status": "COMPLETED",
            "message": f"Synced {len(db_records)} tickets successfully."
        }).eq("id", log_id).execute()

        print(f"✅ BACKGROUND JOB COMPLETED for log_id: {log_id}")

    except Exception as e:
        # 5. エラーハンドリング: ログをエラーに更新
        error_message = f"An error occurred: {str(e)}"
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
        context = browser.new_context(accept_downloads=True)
        context.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,css}", lambda route: route.abort())
        page = context.new_page()
        page.on("dialog", lambda dialog: dialog.accept())
        
        print("👉 Logging in to PC site...")
        page.goto("https://www.nvinq.jra.go.jp/jra/")
        page.wait_for_selector("#UID")
        page.locator("#UID").fill(creds.subscriber_number)
        page.locator("#PWD").fill(creds.password)
        page.locator("#PARS").fill(creds.pars_number)
        page.locator("input[type='submit'][value='ログイン']").click()
        page.wait_for_load_state("networkidle")

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