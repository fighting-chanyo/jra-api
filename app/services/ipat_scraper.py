import os
from fastapi import HTTPException
from playwright.sync_api import sync_playwright
from app.schemas import IpatLoginRequest
from app.services.parsers import parse_jra_csv

# --- 1. 通常同期 (IPAT: 当日/前日) ---
def sync_recent_history(creds: IpatLoginRequest):
    print(f"🔄 Processing Recent History for User: {creds.inet_id}")
    result_data = [] 

    try:
        with sync_playwright() as p:
            is_headless = os.getenv("HEADLESS", "true").lower() != "false"
            iphone = p.devices['iPhone 12']
            browser = p.chromium.launch(headless=is_headless, slow_mo=1000)
            context = browser.new_context(**iphone)
            page = context.new_page()

            # 1. ログイン処理
            print("🚀 Accessing JRA IPAT (SP)...")
            page.goto("https://www.ipat.jra.go.jp/sp/")
            
            if page.locator("input[name='inetid']").is_visible():
                page.locator("input[name='inetid']").fill(creds.inet_id)
                if page.locator("a[onclick*='DoLogin']").is_visible():
                    page.locator("a[onclick*='DoLogin']").click()
                else:
                    page.keyboard.press("Enter")
            
            print("👉 Entering Credentials...")
            page.wait_for_selector("#userid")
            page.locator("#userid").fill(creds.subscriber_no)
            page.locator("#password").fill(creds.pin)
            page.locator("#pars").fill(creds.p_ars)
            
            print("👉 Logging in...")
            page.evaluate("ToSPMenu()")
            page.wait_for_timeout(5000)

            if "投票メニュー" not in page.title() and page.locator(".ui-title").first.inner_text() != "投票メニュー":
                print("❌ Login Failed")
                raise HTTPException(status_code=401, detail="Login Failed")
            print("✅ Login Success")

            # 2. 照会メニューへ
            print("👉 Navigating to Inquiry Menu...")
            page.get_by_text("照会メニュー").click()
            page.wait_for_timeout(2000)

            # 3. 投票内容照会へ
            print("👉 Navigating to History List...")
            if page.locator("#receiptNumber").is_visible():
                page.locator("#receiptNumber").click()
            else:
                page.get_by_text("投票内容照会(当日分/前日分)", exact=True).click()
            
            page.wait_for_timeout(2000)

            # 4. 受付番号一覧
            print("👉 Checking Receipt List...")
            try:
                page.wait_for_selector("ul.receiptNumList li a", timeout=5000)
            except:
                print("⚠️ No receipts found. Maybe no bets today?")
                return {"status": "success", "message": "No bets found", "data": []}

            receipt_count = page.locator("ul.receiptNumList li a").count()
            print(f"👀 Found {receipt_count} receipts.")

            for i in range(receipt_count):
                print(f"   📂 Processing Receipt {i+1}/{receipt_count}...")
                page.locator("ul.receiptNumList li a").nth(i).click()
                page.wait_for_timeout(2000)

                try:
                    page.wait_for_selector("ul.voteList li a", timeout=5000)
                except:
                    print("      ⚠️ No vote list found in this receipt.")
                    page.locator(".headerNavLeftArrow a").click()
                    continue

                vote_count = page.locator("ul.voteList li a").count()
                print(f"      👀 Found {vote_count} bet sets.")

                for j in range(vote_count):
                    page.locator("ul.voteList li a").nth(j).click()
                    page.wait_for_timeout(1000)

                    # 詳細データの取得（一旦HTML保存）
                    # 必要ならここで parse_past_detail_html を呼べるように調整してください
                    print(f"         ✅ Detail page accessed.")
                    
                    page.locator(".headerNavLeftArrow a").click()
                    page.wait_for_timeout(1000)

                print("      🔙 Back to Receipt List...")
                page.locator(".headerNavLeftArrow a").click()
                page.wait_for_timeout(2000)

            print("✨ All Done!")
            browser.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "success", "message": "Recent sync finished", "data": result_data}


# --- 2. 過去分同期 (PCサイト/CSV版: 過去60日) ---
def sync_past_history(creds: IpatLoginRequest):
    print("🚀 Accessing JRA Vote Inquiry (PC/CSV Mode)...")
    result_data = []

    try:
        with sync_playwright() as p:
            is_headless = os.getenv("HEADLESS", "true").lower() != "false"
            # PCサイトのためUserAgent等はデフォルトでOKだが、一応スマホ偽装は解除する
            browser = p.chromium.launch(headless=is_headless)
            context = browser.new_context(accept_downloads=True) # ダウンロード許可
            
            # 不要リソースブロック
            context.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,css}", lambda route: route.abort())
            
            page = context.new_page()
            page.on("dialog", lambda dialog: dialog.accept())

            # 1. ログイン (PCサイト)
            print("👉 Logging in to PC site...")
            page.goto("https://www.nvinq.jra.go.jp/jra/")

            # 修正: セレクタを厳密にする (type='text' または id指定)
            page.wait_for_selector("#UID")
            page.locator("#UID").fill(creds.subscriber_no)
            page.locator("#PWD").fill(creds.pin)
            page.locator("#PARS").fill(creds.p_ars)
            
            # ログインボタンクリック
            page.locator("input[type='submit'][value='ログイン']").click()
            page.wait_for_load_state("networkidle")
            
            # 2. メニュー画面 -> 投票内容照会
            print("👉 Navigating to Vote Inquiry (JRAWeb320)...")
            
            # 修正: 複数の候補が見つかった場合に最初の一つ（投票内容照会）を選択する
            # "投票内容照会" を含む行の中の submit ボタン
            menu_btn = page.locator("tr:has-text('投票内容照会') input[type='submit']").first
            
            if not menu_btn.is_visible():
                print("❌ '投票内容照会' button not found (Strategy 1). Trying Strategy 2...")
                # 代替案: 単純に最初の "選択" ボタンを押す (順番が変わらなければ有効)
                # 投票内容照会は一番上にあるはず
                menu_btn = page.locator("input[value='選択']").first
            
            if not menu_btn.is_visible():
                print("❌ '投票内容照会' button not found. Login failed?")
                with open("debug_login_failed.html", "w", encoding="utf-8") as f: f.write(page.content())
                raise HTTPException(status_code=401, detail="Login Failed or Menu Changed")
            
            menu_btn.click()
            page.wait_for_load_state("networkidle")
            
            # 3. 開催選択 (JRAWeb320) -> 受付番号選択画面(日付選択)へ
            print("👉 Navigating to Receipt Number List (JRAWeb020)...")
            
            # 「受付番号から確認」リンクを探す
            accept_link = page.locator("a.toAcceptnoNum")
            
            if accept_link.is_visible():
                accept_link.click()
            else:
                print("⚠️ Link '受付番号から確認' not visible. Trying form submit...")
                # リンクが見つからない場合、フォームを直接サブミット
                page.evaluate("document.forms['Go020'].submit()")
            
            page.wait_for_load_state("networkidle")

            # 4. 日付選択 (JRAWeb020)
            print("👉 Checking Date List...")
            
            date_buttons = page.locator("input[value='選択']")
            date_count = date_buttons.count()
            print(f"👀 Found {date_count} date buttons.")
            
            if date_count == 0:
                print("⚠️ No dates found. Maybe no history?")
                # --- ここからデバッグコード ---
                debug_filename = "debug_date_page.html"
                with open(debug_filename, "w", encoding="utf-8") as f:
                    f.write(page.content())
                print(f"📄 Saved current page to '{debug_filename}' for debugging.")
                # --- ここまでデバッグコード ---
                browser.close() # ブラウザを閉じて終了
                return {"status": "success", "message": "No past data", "data": []}

            # 日付ごとにループ
            for i in range(date_count):
                print(f"   📅 Processing Date {i+1}/{date_count}...")
                
                # 要素がStaleになるのを防ぐため再取得
                target_btn = page.locator("input[value='選択']").nth(i)
                target_btn.click()
                page.wait_for_load_state("networkidle")

                # 4. 受付番号選択画面 (JRAWeb030) -> CSVダウンロード
                print("      👉 Downloading CSV...")
                
                # CSVダウンロードボタン: form[action*='JRACSVDownload'] input[name='normal']
                csv_btn = page.locator("form[action*='JRACSVDownload'] input[name='normal']")
                
                if csv_btn.is_visible():
                    try:
                        with page.expect_download() as download_info:
                            csv_btn.click()
                        
                        download = download_info.value
                        # 一時ファイルに保存
                        csv_path = f"temp_history_{i}.csv"
                        download.save_as(csv_path)
                        print(f"      ✅ CSV Saved: {csv_path}")
                        
                        # CSV解析
                        parsed = parse_jra_csv(csv_path)
                        result_data.extend(parsed)
                        
                        # 一時ファイル削除
                        os.remove(csv_path)

                    except Exception as e:
                        print(f"      ❌ CSV Download failed: {e}")
                else:
                    print("      ⚠️ CSV Download button not found.")
                
                # 5. 日付リストに戻る
                print("      🔙 Back to Date List...")
                
                # JRAWeb020 (日付選択) に戻るボタンを探す
                # "日付選択" という値のボタンがあるか確認
                # 修正: ボタンが2つあるため、最初に見つかったものを対象にする
                back_btn = page.locator("input[value*='日付選択']").first
                if back_btn.is_visible():
                    back_btn.click()
                else:
                    # なければブラウザバック
                    page.go_back()
                
                page.wait_for_load_state("networkidle")

            browser.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        # デバッグ用
        # import traceback
        # traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "success", "data": result_data}