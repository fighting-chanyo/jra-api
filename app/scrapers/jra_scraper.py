import os
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from app.schemas import IpatAuth
from app.services.parsers import parse_jra_csv
from app.constants import BET_TYPE_MAP

def scrape_past_history_csv(creds: IpatAuth):
    """PlaywrightによるスクレイピングとCSVパース処理を担う (旧sync_past_history)"""
    print("🚀 Accessing JRA Vote Inquiry (PC/CSV Mode)...")
    all_parsed_data = []
    
    with sync_playwright() as p:
        is_headless = os.getenv("HEADLESS", "true").lower() != "false"
        browser = p.chromium.launch(
            headless=is_headless,
            args=["--disable-cache", "--disk-cache-size=0"]
        )
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

def _parse_recent_detail_html(html_content, receipt_no, date_str):
    """直近投票履歴詳細HTMLをパースする"""
    soup = BeautifulSoup(html_content, "html.parser")
    parsed_tickets = []
    
    rows = soup.select("table.table-result tbody tr")
    line_counter = 1
    
    for row in rows:
        # Skip header/footer rows or empty rows
        classes = row.get("class", [])
        if "list-footer" in classes or "print-only" in classes:
            continue
            
        # Check if it's a data row (has td.race-info)
        race_info_td = row.select_one("td.race-info")
        if not race_info_td:
            continue
            
        text_content = race_info_td.get_text(separator=" ", strip=True)
        
        # Parse Place
        # "中京 （土） 8R" -> "中京"
        place_match = re.search(r"^([^\s]+)", text_content)
        race_place = place_match.group(1) if place_match else "Unknown"
        
        # Parse Weekday and Calculate Date
        # "中京 （土） 8R" -> "土"
        weekday_match = re.search(r"（(.)）", text_content)
        race_weekday_str = weekday_match.group(1) if weekday_match else None
        
        calculated_date_str = date_str
        if race_weekday_str:
            try:
                # 節のアンカー日（土曜日）を基準に日付を決定するロジック
                # 1. スクレイプ実行日(D)を基準にする
                scrape_date = datetime.now()
                scrape_weekday = scrape_date.weekday() # Mon=0, ..., Sun=6
                
                # 2. Dに最も近い土曜日(S)を求める
                # 土曜=5. offset = (5 - scrape_weekday + 3) % 7 - 3
                # 月(0) -> -2 (前の土曜), 金(4) -> +1 (次の土曜), 土(5) -> 0, 日(6) -> -1 (前の土曜)
                offset_to_saturday = (5 - scrape_weekday + 3) % 7 - 3
                anchor_saturday = scrape_date + timedelta(days=offset_to_saturday)
                
                # 3. レース開催日のオフセットを計算
                # 金: -1, 土: 0, 日: 1, 月: 2, 火: 3
                target_weekday_map = {'金': -1, '土': 0, '日': 1, '月': 2, '火': 3}
                
                if race_weekday_str in target_weekday_map:
                    day_diff = target_weekday_map[race_weekday_str]
                    race_date = anchor_saturday + timedelta(days=day_diff)
                    calculated_date_str = race_date.strftime("%Y%m%d")
                else:
                    # フォールバック: 従来のマッピング（水・木など）
                    # 基本的にあり得ないが、念のため当日か未来の直近の日付とする
                    weekday_map = {'水': 2, '木': 3}
                    if race_weekday_str in weekday_map:
                        base_weekday = scrape_weekday
                        target_weekday = weekday_map[race_weekday_str]
                        diff_days = (target_weekday - base_weekday + 7) % 7
                        race_date = scrape_date + timedelta(days=diff_days)
                        calculated_date_str = race_date.strftime("%Y%m%d")
            except Exception as e:
                print(f"⚠️ Date calculation failed: {e}")

        # Parse Race No
        race_no_match = re.search(r"(\d+)R", text_content)
        race_number_str = race_no_match.group(1) if race_no_match else "00"
        
        # Parse Bet Type
        # Try to find specific span for bet type first
        bet_type_span = race_info_td.select_one("span.space-2")
        bet_type_raw = bet_type_span.get_text(strip=True) if bet_type_span else "Unknown"
        
        # Map to English code
        bet_type_code = "unknown"
        for jp, en in BET_TYPE_MAP.items():
            if jp in bet_type_raw:
                bet_type_code = en
                break
        
        # Parse Buy Type (Method)
        buy_type_raw = "通常"
        # The buy type is usually in the span after bet type, or just text.
        element_blocks = race_info_td.select("span.element-block")
        if len(element_blocks) >= 3:
             buy_type_raw = element_blocks[-1].get_text(strip=True)
        elif "流し" in text_content or "ながし" in text_content: buy_type_raw = "ながし"
        elif "ボックス" in text_content: buy_type_raw = "ボックス"
        elif "フォーメーション" in text_content: buy_type_raw = "フォーメーション"

        method = "NORMAL"
        multi = False
        if "ＢＯＸ" in buy_type_raw or "ボックス" in buy_type_raw:
            method = "BOX"
        elif "フォーメーション" in buy_type_raw:
            method = "FORMATION"
        elif "ながし" in buy_type_raw or "流し" in buy_type_raw:
            method = "NAGASHI"
            if "マルチ" in buy_type_raw:
                multi = True
        
        # Content Parsing using .print-only
        axis = []
        partners = []
        selections = []
        positions = []
        
        horse_combi_td = row.select_one("td.horse-combi")
        print_only_div = horse_combi_td.select_one(".print-only")
        
        parsed_from_print_only = False

        if print_only_div:
            # Try to parse from .print-only section (for complex bets)
            flex_rows = print_only_div.select(".flex")
            if flex_rows:
                parsed_from_print_only = True
                if method == "NAGASHI":
                    for flex in flex_rows:
                        prefix = flex.select_one(".method-prefix")
                        val_div = flex.select_one(".ng-binding")
                        if prefix and val_div:
                            p_text = prefix.get_text(strip=True)
                            v_text = val_div.get_text(strip=True)
                            nums = [x.strip() for x in v_text.replace(" ", "").split(",") if x.strip()]
                            
                            if "相手" in p_text:
                                partners.extend(nums)
                            else:
                                # "1着:", "軸:", "1頭目:" etc. treat as axis
                                
                                # Parse position from prefix for Fixed Nagashi
                                current_positions = []
                                if not multi:
                                    # Regex to find 1-3 (half or full width) followed by 着 or 頭目
                                    pos_match = re.search(r"([123１２３・]+)(?:着|頭目)", p_text)
                                    if pos_match:
                                        pos_str = pos_match.group(1)
                                        pos_map = {"1": 1, "2": 2, "3": 3, "１": 1, "２": 2, "３": 3}
                                        for char in pos_str:
                                            if char in pos_map:
                                                current_positions.append(pos_map[char])
                                
                                if not current_positions:
                                    axis.extend(nums)
                                else:
                                    # If positions found, add axis and positions for each
                                    for pos in current_positions:
                                        axis.extend(nums)
                                        positions.extend([pos] * len(nums))
                elif method == "FORMATION":
                    # Formation usually lists selections for each position
                    for flex in flex_rows:
                        val_div = flex.select_one(".ng-binding")
                        if val_div:
                            v_text = val_div.get_text(strip=True)
                            nums = [x.strip() for x in v_text.replace(" ", "").split(",") if x.strip()]
                            selections.append(nums)
                else:
                    # Fallback for other types if they appear in flex
                    pass
            
            # If not flex, maybe just text (e.g. BOX)
            if not parsed_from_print_only:
                text = print_only_div.get_text(strip=True)
                if text:
                    # Simple extraction of numbers
                    nums = re.findall(r"\d+", text)
                    if nums:
                        parsed_from_print_only = True
                        if method == "BOX":
                            selections.append(nums)
                        elif method == "NORMAL":
                            selections.append(nums)

        # Fallback if print-only didn't yield results (or for Normal bets where print-only might be empty)
        if not parsed_from_print_only:
             # Use horse-combi-list
             combi_spans = horse_combi_td.select("span.set-heading")
             if combi_spans:
                 nums = [s.get_text(strip=True) for s in combi_spans]
                 selections.append(nums)
             else:
                 # Text fallback
                 text = horse_combi_td.get_text(strip=True)
                 nums = re.findall(r"\d+", text)
                 if nums:
                    selections.append(nums)

        # Extract Amount per point
        money_td = row.select_one("td.money")
        amount_per_point = 0
        if money_td:
            # The first div.ng-binding contains the "per point" amount (e.g., "200円")
            money_div = money_td.select_one("div.ng-binding")
            if money_div:
                amount_text = money_div.get_text(strip=True).replace("円", "").replace(",", "")
                try:
                    amount_per_point = int(amount_text)
                except:
                    amount_per_point = 0
        
        if amount_per_point == 0:
            amount_per_point = 100 # Default fallback

        # Sets (points)
        sets_td = row.select_one("td.sets")
        total_points = 0
        if sets_td:
            sets_text = sets_td.get_text(strip=True).replace("組", "").replace(",", "")
            try:
                total_points = int(sets_text)
            except:
                total_points = 0
        
        # If points couldn't be parsed, assume at least 1
        if total_points == 0:
            total_points = 1

        # Calculate total_cost strictly as (amount_per_point * total_points)
        calculated_total_cost = amount_per_point * total_points

        payout = 0
        status = "PENDING"
        
        ticket = {
            "raw": {
                "race_date_str": calculated_date_str,
                "race_place": race_place,
                "race_number_str": race_number_str,
                "receipt_no": receipt_no,
                "line_no": line_counter
            },
            "parsed": {
                "bet_type": bet_type_code,
                "buy_type": method,
                "content": {
                    "type": bet_type_code,
                    "method": method,
                    "multi": multi,
                    "axis": axis,
                    "partners": partners,
                    "selections": selections,
                    "positions": positions
                },
                "amount_per_point": amount_per_point,
                "total_points": total_points,
                "total_cost": calculated_total_cost,
                "payout": payout,
                "status": status,
                "source": "IPAT_RECENT",
                "mode": "REAL"
            }
        }
        
        parsed_tickets.append(ticket)
        line_counter += 1
        
    return parsed_tickets

def scrape_recent_history(creds: IpatAuth):
    """Playwrightによるスクレイピング (Recent History Mode)"""
    print("🚀 Accessing JRA IPAT (Recent History Mode)...")
    all_parsed_data = []
    
    with sync_playwright() as p:
        is_headless = os.getenv("HEADLESS", "true").lower() != "false"
        browser = p.chromium.launch(
            headless=is_headless,
            args=["--disable-cache", "--disk-cache-size=0"]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        # Step 1: Top Page (INET-ID)
        print("👉 Logging in to IPAT (Step 1: INET-ID)...")
        page.goto("https://www.ipat.jra.go.jp/")
        
        # Check for closed message
        if page.locator("text=ただいまの時間は投票受付時間外です。").is_visible():
             raise Exception("JRA IPAT is currently closed.")
        
        inet_id = creds.inet_id.strip()
        if not inet_id:
             raise Exception("INET-ID is missing")

        page.fill("input[name='inetid']", inet_id)
        with page.expect_navigation(wait_until="domcontentloaded"):
            page.click("p.button a[title='ログイン']")
            
        # Step 2: Subscriber Info
        print("👉 Logging in to IPAT (Step 2: Subscriber Info)...")
        page.wait_for_selector("input[name='i']")
        page.fill("input[name='i']", creds.subscriber_number.strip())
        page.fill("input[name='p']", creds.password.strip())
        page.fill("input[name='r']", creds.pars_number.strip())
        with page.expect_navigation(wait_until="domcontentloaded"):
            page.click("a[title='ネット投票メニューへ']")

        # Step 3: Go to Vote History (Recent)
        print("👉 Logging in to IPAT (Step 3: Vote History)...")
        page.wait_for_load_state("networkidle")
        history_btn_selector = "button.btn-reference"
        page.wait_for_selector(history_btn_selector)
        page.click(history_btn_selector)
        page.wait_for_selector("h1:has-text('投票履歴一覧')")

        # Step 4: Iterate through Today and Yesterday
        print("👉 Checking for history items (Today & Yesterday)...")
        
        target_days = [
            ("Today", "label[for='refer-today']", 0),
            ("Yesterday", "label[for='refer-before']", 1)
        ]
        
        for day_name, selector, day_offset in target_days:
            print(f"👉 Switching to {day_name}...")
            page.click(selector)
            
            try:
                page.wait_for_selector("div.list-loading", state="visible", timeout=1000)
                page.wait_for_selector("div.list-loading", state="hidden", timeout=10000)
            except:
                page.wait_for_timeout(1000)
            
            # Calculate Date String (YYYYMMDD)
            target_date = datetime.now() - timedelta(days=day_offset)
            date_str = target_date.strftime("%Y%m%d")

            # Get number of receipts
            rows = page.locator("table.table-status tbody tr")
            count = rows.count()
            print(f"👀 Found {count} history items for {day_name}.")
            
            for i in range(count):
                # Re-query rows to avoid stale elements
                rows = page.locator("table.table-status tbody tr")
                row = rows.nth(i)
                
                # Check for "No History" message
                try:
                    row_text = row.inner_text()
                    if "投票履歴がありません" in row_text:
                        print(f"   ℹ️ No history found for {day_name}. Skipping.")
                        break
                except:
                    pass

                # Extract Receipt No
                try:
                    receipt_no = row.locator("td.receipt a").inner_text().strip()
                except:
                    print(f"⚠️ Could not extract receipt no for row {i}")
                    # Only print HTML if it's not the "No history" row (which we should have caught above, but just in case)
                    if "投票履歴がありません" not in row.inner_html():
                        print(f"   Row HTML: {row.inner_html()}")
                    continue

                print(f"   Processing Receipt: {receipt_no}")
                
                # Click to open details
                try:
                    target_link = row.locator("td.receipt a")
                    target_link.scroll_into_view_if_needed()
                    target_link.click()
                except Exception as e:
                    print(f"   ⚠️ Failed to click receipt {receipt_no}: {e}")
                    continue

                # Wait for detail view
                is_detail_loaded = False
                try:
                    # Wait for either the header or a known element in the detail view
                    page.wait_for_selector("h1:has-text('投票履歴結果内容'), table.table-result", timeout=10000)
                    is_detail_loaded = True
                except:
                    print(f"   ⚠️ Failed to load detail view for {receipt_no}. Attempting to recover...")
                
                if is_detail_loaded:
                    # Parse Detail HTML
                    content = page.content()
                    try:
                        parsed = _parse_recent_detail_html(content, receipt_no, date_str)
                        all_parsed_data.extend(parsed)
                        print(f"   ✅ Extracted {len(parsed)} tickets.")
                    except Exception as e:
                        print(f"   ❌ Error parsing detail for {receipt_no}: {e}")
                
                # Go back to list (Always try to go back if we might have moved)
                try:
                    # Try "一覧に戻る" button
                    back_btn = page.locator("button:has-text('一覧に戻る')")
                    if back_btn.is_visible():
                        back_btn.click()
                    else:
                        # Fallback: Close detail modal if it's a modal
                        close_btn = page.locator("button:has-text('閉じる')").last
                        if close_btn.is_visible():
                            close_btn.click()
                    
                    # Wait for list to reappear
                    page.wait_for_selector("h1:has-text('投票履歴一覧')", timeout=5000)
                except:
                    # If we can't find the back button or list header, we might already be on the list or stuck
                    if "投票履歴一覧" not in page.content():
                        print(f"   ⚠️ Could not confirm return to list for {receipt_no}. Reloading page...")
                        page.reload()
                        page.wait_for_selector("h1:has-text('投票履歴一覧')")
                
        browser.close()
        
    return all_parsed_data
