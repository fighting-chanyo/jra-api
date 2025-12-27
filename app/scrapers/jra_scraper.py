import os
import re
import time
from datetime import datetime, timedelta
import logging
import threading
from contextlib import contextmanager
from typing import Optional
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from app.schemas import IpatAuth
from app.services.parsers import parse_jra_csv
from app.constants import BET_TYPE_MAP

logger = logging.getLogger(__name__)


_DIGITS_RE = re.compile(r"[0-9０-９]+")


_JP_WEEKDAY_TO_PY = {"月": 0, "火": 1, "水": 2, "木": 3, "金": 4, "土": 5, "日": 6}


def _infer_recent_race_date_from_weekday(
    reference_yyyymmdd: str,
    jp_weekday: str,
    *,
    prefer_future: bool,
    max_days: int = 7,
) -> Optional[str]:
    """recent詳細には開催日の明示が無いので、参照日と曜日から開催日を推定する。

    - Yesterdayタブ: 未来馬券は出ない前提のため「過去優先」
    - Todayタブ: 当日・翌日など未来馬券が混ざり得るため「未来優先」

    max_days は「1週間後など遠未来」を誤推定しないための上限。
    """
    if not reference_yyyymmdd or not jp_weekday:
        return None
    target_weekday = _JP_WEEKDAY_TO_PY.get(jp_weekday)
    if target_weekday is None:
        return None
    try:
        ref = datetime.strptime(reference_yyyymmdd, "%Y%m%d")
    except Exception:
        return None

    def _search(deltas) -> Optional[str]:
        for delta in deltas:
            cand = ref + timedelta(days=delta)
            if cand.weekday() == target_weekday:
                return cand.strftime("%Y%m%d")
        return None

    if prefer_future:
        # 参照日以降(当日含む)を優先し、見つからなければ直近過去
        found = _search(range(0, max_days + 1))
        if found:
            return found
        return _search(range(-1, -(max_days + 1), -1))

    # 過去優先（参照日含む）
    found = _search(range(0, -(max_days + 1), -1))
    if found:
        return found
    return _search(range(1, max_days + 1))


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or str(default))
    except Exception:
        return default


def _mask_digits(text: str) -> str:
    if not text:
        return text
    return _DIGITS_RE.sub("X", text)


_PLAYWRIGHT_MAX_CONCURRENCY = int(os.getenv("PLAYWRIGHT_MAX_CONCURRENCY", "1") or "1")
_PLAYWRIGHT_SLOT_TIMEOUT_SEC = float(os.getenv("PLAYWRIGHT_SLOT_TIMEOUT_SEC", "30") or "30")
_PLAYWRIGHT_SEMAPHORE = threading.BoundedSemaphore(_PLAYWRIGHT_MAX_CONCURRENCY)


@contextmanager
def _playwright_slot():
    acquired = _PLAYWRIGHT_SEMAPHORE.acquire(timeout=_PLAYWRIGHT_SLOT_TIMEOUT_SEC)
    if not acquired:
        raise Exception(
            "Playwright is busy (too many concurrent sessions in this instance). "
            "Please retry in a moment."
        )
    try:
        yield
    finally:
        _PLAYWRIGHT_SEMAPHORE.release()


def _route_block_heavy_assets_pc(route):
    try:
        req = route.request
        if req.resource_type in ("image", "media", "font"):
            route.abort()
            return
        url = (req.url or "").lower()
        if url.endswith(
            (
                ".png",
                ".jpg",
                ".jpeg",
                ".gif",
                ".webp",
                ".svg",
                ".woff",
                ".woff2",
                ".ttf",
                ".otf",
                ".css",
                ".mp4",
                ".webm",
                ".mp3",
                ".m4a",
                ".ogg",
            )
        ):
            route.abort()
            return
        route.continue_()
    except Exception:
        # ルーティング処理で例外を投げるとナビゲーションが不安定になるため握りつぶす
        try:
            route.continue_()
        except Exception:
            pass


def _route_block_heavy_assets_modern(route):
    """Recent(modern)向け: 画像/フォント/メディアは止めるがCSSは止めない。

    modern側はボタンがCSS前提のことがあり、CSSを止めると要素が不可視になって
    Playwrightのclickがタイムアウトすることがある。
    """
    try:
        req = route.request
        # 直近サイトは画像を止めるとUIが壊れるケースがあるため、
        # デフォルトは font/media のみブロックし、画像ブロックは明示指定にする。
        block_images = _env_bool("IPAT_BLOCK_IMAGES", False)

        rtype = getattr(req, "resource_type", None)
        if rtype in ("media", "font"):
            route.abort()
            return
        if block_images and rtype == "image":
            route.abort()
            return
        route.continue_()
    except Exception:
        try:
            route.continue_()
        except Exception:
            pass


def scrape_past_history_csv(creds: IpatAuth):
    """PlaywrightによるスクレイピングとCSVパース処理を担う (旧sync_past_history)"""
    logger.info("Accessing JRA Vote Inquiry (PC/CSV Mode)...")
    all_parsed_data = []

    with _playwright_slot():
        with sync_playwright() as p:
            is_headless = os.getenv("HEADLESS", "true").lower() != "false"
            browser = p.chromium.launch(
                headless=is_headless,
                args=[
                    "--disable-cache",
                    "--disk-cache-size=0",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            )
            context.route("**/*", _route_block_heavy_assets_pc)
            page = context.new_page()
            page.on("dialog", lambda dialog: dialog.accept())

            try:
                logger.info("Logging in to PC site...")
                page.goto("https://www.nvinq.jra.go.jp/jra/")

                with open("debug_login_page.html", "w", encoding="utf-8") as f:
                    f.write(page.content())

                s_no = creds.subscriber_number.strip()
                pwd = creds.password.strip()
                pars = creds.pars_number.strip()

                page.wait_for_selector("#UID")
                page.locator("#UID").fill(s_no)
                page.wait_for_timeout(500)
                page.locator("#PWD").fill(pars)
                page.wait_for_timeout(500)
                page.locator("#PARS").fill(pwd)
                page.wait_for_timeout(500)
                page.locator("input[type='submit'][value='ログイン']").click()
                page.wait_for_load_state("networkidle")

                if page.locator("text=加入者番号・暗証番号・P-ARS番号に誤りがあります").is_visible():
                    with open("debug_login_failed.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    raise Exception(
                        "Login Failed: Invalid Credentials (加入者番号・暗証番号・P-ARS番号に誤りがあります)"
                    )

                logger.info("Navigating to Vote Inquiry (JRAWeb320)...")
                menu_btn = page.locator("tr:has-text('投票内容照会') input[type='submit']").first
                if not menu_btn.is_visible():
                    menu_btn = page.locator("input[value='選択']").first
                if not menu_btn.is_visible():
                    with open("debug_login_failed.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    raise Exception("Login Failed or Menu Changed. See debug_login_failed.html")
                menu_btn.click()
                page.wait_for_load_state("networkidle")

                logger.info("Navigating to Receipt Number List (JRAWeb020)...")
                accept_link = page.locator("a.toAcceptnoNum")
                if accept_link.is_visible():
                    with page.expect_navigation(wait_until="domcontentloaded"):
                        accept_link.click()
                else:
                    with page.expect_navigation(wait_until="domcontentloaded"):
                        page.evaluate("document.forms['Go020'].submit()")

                try:
                    page.wait_for_load_state("domcontentloaded", timeout=15000)
                    page.locator("h2:has-text('日付選択')").wait_for(timeout=15000)
                    if page.locator("text=ログインが無効となったか").is_visible():
                        raise Exception("Session timed out or became invalid. Please try again.")
                except Exception as e:
                    error_message = str(e) if str(e) else "Failed to determine page state after navigation."
                    with open("debug_navigation_error.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    raise Exception(error_message)

                logger.info("Checking Date List...")
                date_buttons = page.locator("table[border='1'] input[type='submit'][value='選択']")
                if date_buttons.count() == 0:
                    date_buttons = page.locator("input[type='submit'][value='選択']")
                date_count = date_buttons.count()
                logger.info("Found %d date buttons.", date_count)

                if date_count == 0:
                    try:
                        logger.info("No dates found. title='%s' url='%s'", page.title(), page.url)
                    except Exception:
                        logger.info("No dates found. (failed to read title/url)")
                    with open("debug_date_list_missing.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    logger.info(
                        "No dates found. Maybe no betting history or unexpected page. See debug_date_list_missing.html"
                    )
                    return []

                for i in range(date_count):
                    date_buttons.nth(i).click()
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

            finally:
                try:
                    context.close()
                finally:
                    browser.close()

    return all_parsed_data


def _parse_recent_detail_html(html_content, receipt_no, date_str, prefer_future: bool):
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
            inferred = _infer_recent_race_date_from_weekday(
                date_str,
                race_weekday_str,
                prefer_future=prefer_future,
            )
            if inferred:
                calculated_date_str = inferred

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
    logger.info("Accessing JRA IPAT (Recent History Mode)...")
    all_parsed_data = []

    with _playwright_slot():
        with sync_playwright() as p:
            # Cloud Run等のXが無い環境で headed を起動すると即死するため、
            # HEADLESS のデフォルトは True（headless）にする。
            is_headless = _env_bool("HEADLESS", True)
            if not is_headless and not (os.getenv("DISPLAY") or ""):
                logger.warning("$DISPLAY is not set; forcing headless browser.")
                is_headless = True
            slow_mo_ms = _env_int("PLAYWRIGHT_SLOW_MO_MS", 0)
            browser = p.chromium.launch(
                headless=is_headless,
                slow_mo=slow_mo_ms if slow_mo_ms > 0 else None,
                args=[
                    "--disable-cache",
                    "--disk-cache-size=0",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            )
            disable_blocking = _env_bool("DISABLE_RESOURCE_BLOCKING", False)
            if disable_blocking:
                logger.info("Resource blocking disabled (DISABLE_RESOURCE_BLOCKING=true)")
            else:
                context.route("**/*", _route_block_heavy_assets_modern)
            page = context.new_page()

            save_artifacts = _env_bool("SAVE_DEBUG_ARTIFACTS", False)
            trace_enabled = _env_bool("IPAT_TRACE", False)
            trace_path = os.getenv("IPAT_TRACE_PATH", "/tmp/ipat_recent_trace.zip")
            pause_at = (os.getenv("IPAT_DEBUG_PAUSE_AT", "") or "").strip()

            # Heavy diagnostics are off by default; enable explicitly when debugging.
            debug_log = _env_bool("IPAT_DEBUG_LOG", False) or save_artifacts or trace_enabled or bool(pause_at)
            debug_log_console = _env_bool("IPAT_DEBUG_LOG_CONSOLE", False) or False
            debug_log_frames = _env_bool("IPAT_DEBUG_LOG_FRAMES", False) or False
            debug_log_requests = _env_bool("IPAT_DEBUG_LOG_REQUESTS", False) or False

            # If IPAT_DEBUG_LOG is on, default to useful subsets.
            if debug_log and not (debug_log_console or debug_log_frames or debug_log_requests):
                debug_log_frames = True
                debug_log_requests = True

            # modern側は load/networkidle 待ちがハングすることがあるため、デフォルトの待ち時間を短めに設定
            try:
                page.set_default_timeout(30000)
            except Exception:
                pass
            try:
                page.set_default_navigation_timeout(30000)
            except Exception:
                pass

            # --- Diagnostics (optional) ---
            if debug_log_requests:
                try:
                    page.on(
                        "requestfailed",
                        lambda req: logger.warning(
                            "requestfailed: method=%s url=%s err=%s",
                            getattr(req, "method", None),
                            _mask_digits(getattr(req, "url", "") or "")[:220],
                            _mask_digits(getattr(getattr(req, "failure", None), "error_text", None) or "")[:200],
                        ),
                    )
                except Exception:
                    pass

            if debug_log_console:
                try:
                    page.on(
                        "console",
                        lambda msg: logger.info(
                            "console[%s]: %s",
                            getattr(msg, "type", ""),
                            _mask_digits(getattr(msg, "text", "") or "")[:300],
                        ),
                    )
                except Exception:
                    pass

                try:
                    page.on(
                        "pageerror",
                        lambda err: logger.warning("pageerror: %s", _mask_digits(str(err))[:300]),
                    )
                except Exception:
                    pass

            if debug_log_frames:
                try:
                    page.on(
                        "framenavigated",
                        lambda frame: logger.info(
                            "framenavigated: name=%s url=%s",
                            getattr(frame, "name", "") or "",
                            _mask_digits(getattr(frame, "url", "") or "")[:220],
                        ),
                    )
                except Exception:
                    pass

            def _goto(url: str, label: str):
                # modern側は `load`/`domcontentloaded` が発火しない・遅いケースがあり、goto待ちが固まりやすい。
                # まず `commit` まで待って「通信できているか」を確定し、domcontentloaded は短めに待ってダメなら先に進む。
                timeout_ms = _env_int("IPAT_GOTO_TIMEOUT_MS", 45000)
                if debug_log_frames:
                    logger.info("goto(%s): %s", label, url)
                resp = page.goto(url, wait_until="commit", timeout=timeout_ms)
                try:
                    status = getattr(resp, "status", None)
                    resp_url = getattr(resp, "url", None)
                    if debug_log_frames:
                        logger.info(
                            "goto(%s) committed. status=%s url=%s",
                            label,
                            status,
                            _mask_digits(str(resp_url or ""))[:220],
                        )
                except Exception:
                    if debug_log_frames:
                        logger.info("goto(%s) committed.", label)

                # domcontentloaded は任意（固まり回避優先）
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=min(timeout_ms, 15000))
                except Exception as e:
                    if debug_log_frames:
                        logger.warning("goto(%s) domcontentloaded not reached (continuing): %s", label, e)

                # Optional artifacts
                if save_artifacts:
                    try:
                        page.screenshot(path=f"/tmp/debug_goto_{label}.png", full_page=True)
                    except Exception:
                        pass
                    try:
                        with open(f"/tmp/debug_goto_{label}.html", "w", encoding="utf-8") as f:
                            f.write(page.content())
                    except Exception:
                        pass

                return resp

            def _debug_pause(label: str):
                # 例: IPAT_DEBUG_PAUSE_AT="step1,step2,history"
                if not pause_at:
                    return
                tokens = {t.strip().lower() for t in pause_at.split(",") if t.strip()}
                if label.lower() in tokens or "all" in tokens:
                    try:
                        if debug_log:
                            logger.info("Debug pause at '%s' (PWDEBUG推奨)", label)
                        page.pause()
                    except Exception:
                        # pauseできない環境でも落とさない
                        pass

            if trace_enabled:
                try:
                    context.tracing.start(screenshots=True, snapshots=True, sources=True)
                    if debug_log:
                        logger.info("Tracing enabled. trace_path='%s'", trace_path)
                except Exception as e:
                    logger.warning("Failed to start tracing: %s", e)

            last_dialog_message = {"message": None}

            popup_page_holder = {"page": None}
            last_popup_info = {"url": None, "title": None}

            def _on_dialog(dialog):
                try:
                    msg = dialog.message
                except Exception:
                    msg = None
                if msg:
                    last_dialog_message["message"] = _mask_digits(str(msg))[:200]
                    logger.warning("IPAT dialog detected (auto-accepted): %s", last_dialog_message["message"])
                try:
                    dialog.accept()
                except Exception:
                    pass

            page.on("dialog", _on_dialog)

            def _on_new_page(new_page):
                try:
                    popup_page_holder["page"] = new_page
                except Exception:
                    pass
                try:
                    new_page.on("dialog", _on_dialog)
                except Exception:
                    pass
                try:
                    last_popup_info["url"] = _mask_digits(getattr(new_page, "url", "") or "")[:200]
                except Exception:
                    pass
                try:
                    last_popup_info["title"] = (new_page.title() or "")[:120]
                except Exception:
                    pass

            # クリック後に別タブ/ポップアップで遷移するケースを拾う
            try:
                context.on("page", _on_new_page)
            except Exception:
                pass

            def _adopt_popup_if_any(reason: str):
                nonlocal page
                try:
                    new_page = popup_page_holder.get("page")
                except Exception:
                    new_page = None
                if new_page and new_page != page:
                    try:
                        if debug_log:
                            logger.info(
                                "Switching to popup page (%s). url='%s' title='%s'",
                                reason,
                                last_popup_info.get("url"),
                                last_popup_info.get("title"),
                            )
                    except Exception:
                        if debug_log:
                            logger.info("Switching to popup page (%s).", reason)
                    page = new_page

            def _count_in_any_frame(selector: str) -> int:
                total = 0
                try:
                    total += page.locator(selector).count()
                except Exception:
                    pass
                try:
                    for frame in page.frames:
                        if frame == page.main_frame:
                            continue
                        try:
                            total += frame.locator(selector).count()
                        except Exception:
                            continue
                except Exception:
                    pass
                return total

            def _click_first_in_any_frame(selector: str, timeout_ms: int = 10000) -> bool:
                try:
                    loc = page.locator(selector)
                    if loc.count() > 0:
                        loc.first.click(timeout=timeout_ms)
                        return True
                except Exception:
                    pass

                try:
                    for frame in page.frames:
                        if frame == page.main_frame:
                            continue
                        try:
                            loc = frame.locator(selector)
                            if loc.count() > 0:
                                loc.first.click(timeout=timeout_ms)
                                return True
                        except Exception:
                            continue
                except Exception:
                    pass

                return False

            def _wait_for_selector_any_frame(selector: str, timeout_ms: int) -> bool:
                deadline = time.time() + (timeout_ms / 1000.0)
                while time.time() < deadline:
                    if _count_in_any_frame(selector) > 0:
                        return True
                    try:
                        page.wait_for_timeout(250)
                    except Exception:
                        time.sleep(0.25)
                return False

            def _is_restart_notice() -> bool:
                # 「初期画面からINET-IDを入力してやり直してください」系の案内
                try:
                    if page.locator("text=初期画面からINET-ID").count() > 0:
                        return True
                except Exception:
                    pass
                try:
                    if page.locator("text=INET-IDを入力してやり直してください").count() > 0:
                        return True
                except Exception:
                    pass
                return False

            class _IpatRestartRequired(Exception):
                pass

            def _best_effort_clear_site_storage():
                """cookie以外のブラウザ状態も可能な範囲でクリアする。

                Cloud Run上での断続的な「初期画面からINET-ID…」(restart notice) は
                cookieだけでは解消しないケースがあるため、local/session storage や
                SW/caches/IndexedDBをベストエフォートで消す。
                """
                try:
                    page.evaluate(
                        """() => {
                            try { localStorage && localStorage.clear(); } catch (e) {}
                            try { sessionStorage && sessionStorage.clear(); } catch (e) {}
                        }"""
                    )
                except Exception:
                    pass

                try:
                    page.evaluate(
                        """() => {
                            try {
                                if (navigator && navigator.serviceWorker && navigator.serviceWorker.getRegistrations) {
                                    navigator.serviceWorker.getRegistrations().then(rs => rs.forEach(r => r.unregister())).catch(() => {});
                                }
                            } catch (e) {}
                            try {
                                if (typeof caches !== 'undefined' && caches.keys) {
                                    caches.keys().then(keys => Promise.all(keys.map(k => caches.delete(k)))).catch(() => {});
                                }
                            } catch (e) {}
                        }"""
                    )
                except Exception:
                    pass

                try:
                    page.evaluate(
                        """() => {
                            try {
                                if (typeof indexedDB !== 'undefined' && indexedDB.databases) {
                                    indexedDB.databases().then(dbs => {
                                        (dbs || []).forEach(db => {
                                            try { if (db && db.name) { indexedDB.deleteDatabase(db.name); } } catch (e) {}
                                        });
                                    }).catch(() => {});
                                }
                            } catch (e) {}
                        }"""
                    )
                except Exception:
                    pass

            try:
                try:
                    login_retries = int(os.getenv("IPAT_RECENT_LOGIN_RETRIES", "1") or "1")
                except Exception:
                    login_retries = 1

                for attempt in range(login_retries + 1):
                    if attempt > 0:
                        logger.warning("Retrying recent login from initial screen. attempt=%d", attempt + 1)
                        try:
                            context.clear_cookies()
                        except Exception:
                            pass
                        try:
                            popup_page_holder["page"] = None
                            last_popup_info["url"] = None
                            last_popup_info["title"] = None
                        except Exception:
                            pass
                        try:
                            _goto("https://www.ipat.jra.go.jp/", "retry-initial")
                            _best_effort_clear_site_storage()
                            page.wait_for_timeout(1200)
                        except Exception:
                            pass

                    try:
                        logger.info("Logging in to IPAT (Step 1: INET-ID)...")
                        _goto("https://www.ipat.jra.go.jp/", "step1")

                        _debug_pause("step1")

                        if page.locator("text=ただいまの時間は投票受付時間外です。").is_visible():
                            raise Exception("JRA IPAT is currently closed.")

                        inet_id = creds.inet_id.strip()
                        if not inet_id:
                            raise Exception("INET-ID is missing")

                        page.fill("input[name='inetid']", inet_id)
                        with page.expect_navigation(wait_until="domcontentloaded"):
                            page.click("p.button a[title='ログイン']")

                        logger.info("Logging in to IPAT (Step 2: Subscriber Info)...")
                        page.wait_for_selector("input[name='i']")

                        _debug_pause("step2")
                        page.fill("input[name='i']", creds.subscriber_number.strip())
                        # past(PC)モードが現在動作している入力対応に合わせる
                        # input[name='p'] と input[name='r'] を入れ替える
                        page.fill("input[name='p']", creds.pars_number.strip())
                        page.fill("input[name='r']", creds.password.strip())
                        before_url = page.url
                        # NOTE: modern側はSPA的な遷移で「navigation」が発生しない場合がある。
                        # expect_navigationで待つとハングすることがあるため、画面要素の出現で待機する。
                        page.wait_for_selector("a[title='ネット投票メニューへ']", timeout=15000)
                        menu_link = page.locator("a[title='ネット投票メニューへ']")
                        try:
                            menu_link.scroll_into_view_if_needed()
                        except Exception:
                            pass
                        try:
                            menu_link.click(timeout=10000)
                        except Exception as e1:
                            logger.warning("Menu link not clickable (normal): %s", e1)
                            try:
                                # 不可視でもonclickを発火させたいケースがある
                                menu_link.click(timeout=5000, force=True)
                            except Exception as e2:
                                logger.warning("Menu link not clickable (force): %s", e2)
                                # 最後の手段: 直接JS関数を呼ぶ
                                page.evaluate(
                                    "if (typeof ToModernMenu === 'function') { ToModernMenu(); } else { throw new Error('ToModernMenu not found'); }"
                                )

                        # 別タブ/ポップアップが開いた場合はそちらに切替
                        try:
                            page.wait_for_timeout(1000)
                        except Exception:
                            pass
                        _adopt_popup_if_any("after menu action")

                        # 画面がJS/非同期で切り替わる場合があるため、一旦待つ
                        try:
                            page.wait_for_load_state("networkidle", timeout=10000)
                        except Exception:
                            pass

                        # ここで「初期画面からINET-IDを…」案内に落ちるケースがあるため早期検知
                        if _is_restart_notice():
                            raise _IpatRestartRequired("restart notice shown after menu action")

                        # まだ加入者入力画面のままなら、フォームsubmitも試す（リンク/JSが効かないケース向け）
                        try:
                            still_on_subscriber = (
                                "pw_080_i.cgi" in (page.url or "")
                                and page.locator("input[name='i'], input[name='p'], input[name='r']").count() > 0
                            )
                        except Exception:
                            still_on_subscriber = False

                        if still_on_subscriber:
                            logger.warning(
                                "Still on subscriber page after menu action. url(before)='%s' url(after)='%s' - trying form submit.",
                                before_url,
                                getattr(page, "url", None),
                            )
                            try:
                                # 入力欄の属するformをsubmitする
                                page.eval_on_selector("input[name='r']", "el => el.form && el.form.submit()")
                            except Exception as e:
                                logger.warning("Form submit fallback failed: %s", e)

                        # submitで別ページが開くパターンもある
                        try:
                            page.wait_for_timeout(1000)
                        except Exception:
                            pass
                        _adopt_popup_if_any("after submit fallback")

                        if _is_restart_notice():
                            raise _IpatRestartRequired("restart notice shown after submit fallback")

                        # メニュー画面に到達したことを、複合条件で判定
                        # - URLが変わる
                        # - 加入者入力欄が消える
                        # - メニュー要素が出る
                        try:
                            page.wait_for_function(
                                """() => {
                                    const urlChanged = !location.href.includes('pw_080_i.cgi');
                                    const hasInputs = !!document.querySelector("input[name='i'], input[name='p'], input[name='r']");
                                    const hasMenuBtn = !!document.querySelector('button.btn-reference');
                                    return urlChanged || !hasInputs || hasMenuBtn;
                                }""",
                                timeout=25000,
                            )
                        except Exception:
                            pass

                        if _is_restart_notice():
                            raise _IpatRestartRequired("restart notice shown during menu wait")

                        # ここでメニュー要素が出る想定（iframe内の可能性もあるため全frame対象）
                        if not _wait_for_selector_any_frame("button.btn-reference", timeout_ms=15000):
                            raise PlaywrightTimeoutError("menu button not found in any frame")

                        logger.info("Logging in to IPAT (Step 3: Vote History)...")
                        page.wait_for_load_state("networkidle")

                        _debug_pause("history")

                        # UI変更/文言差異に備えて複数候補を試す
                        history_candidates = [
                            "button.btn-reference",
                            "a:has-text('投票履歴')",
                            "button:has-text('投票履歴')",
                            "a:has-text('投票履歴一覧')",
                            "button:has-text('投票履歴一覧')",
                        ]

                        clicked = False
                        for sel in history_candidates:
                            if _wait_for_selector_any_frame(sel, timeout_ms=4000):
                                if _click_first_in_any_frame(sel, timeout_ms=15000):
                                    clicked = True
                                    break

                        if not clicked:
                            raise Exception("History entry not found/clickable (main or iframe)")
                        page.wait_for_selector("h1:has-text('投票履歴一覧')")

                        # ここまで来ればログイン成功
                        break

                    except _IpatRestartRequired as e:
                        if attempt < login_retries:
                            logger.warning(
                                "IPAT restart notice detected; will reset and retry. attempt=%d/%d detail=%s",
                                attempt + 1,
                                login_retries + 1,
                                e,
                            )
                            continue
                        raise

                else:
                    raise Exception("Recent login retry loop exhausted")

            except _IpatRestartRequired as e:
                raise Exception(
                    "IPAT returned a restart notice (初期画面からINET-IDを入力してやり直してください). "
                    "The session/state was rejected. Try again later. "
                    f"detail={e}"
                )

            except PlaywrightTimeoutError:
                # 失敗時に、画面内のエラーらしきテキストを(数字マスクして)少しだけ拾う
                error_texts = []
                try:
                    candidates = page.locator(
                        ".error, .err, .caution, .message, #error, #errmsg, [class*='error'], [id*='error']"
                    )
                    n = min(candidates.count(), 3)
                    for i in range(n):
                        t = candidates.nth(i).inner_text().strip()
                        if t:
                            error_texts.append(_mask_digits(t)[:200])
                except Exception:
                    pass

                debug = {
                    "url": getattr(page, "url", None),
                    "title": None,
                    "has_menu_button": _count_in_any_frame("button.btn-reference") > 0,
                    "has_subscriber_inputs": page.locator("input[name='i'], input[name='p'], input[name='r']").count() > 0,
                    "has_login_button": page.locator("p.button a[title='ログイン']").count() > 0,
                    "has_menu_link": page.locator("a[title='ネット投票メニューへ']").count() > 0,
                    "has_error_like_text": page.locator(
                        "text=誤り|text=エラー|text=入力|text=再入力|text=失敗|text=確認|text=有効"
                    ).count()
                    > 0,
                    "error_texts": error_texts,
                    "last_dialog": last_dialog_message.get("message"),
                    "popup_url": last_popup_info.get("url"),
                    "popup_title": last_popup_info.get("title"),
                    "body_text_head": None,
                    "frame_count": None,
                    "frames": [],
                }
                try:
                    debug["title"] = page.title()
                except Exception:
                    debug["title"] = None

                try:
                    # 画面テキストを少しだけ（数字はマスク）
                    body_text = page.locator("body").inner_text(timeout=1000)
                    debug["body_text_head"] = _mask_digits(body_text).strip().replace("\n\n", "\n")[:400]
                except Exception:
                    debug["body_text_head"] = None

                try:
                    frames = page.frames
                    debug["frame_count"] = len(frames)
                    # URL等は念のため短くする
                    for fr in frames[:5]:
                        try:
                            fr_url = getattr(fr, "url", "") or ""
                            debug["frames"].append(
                                {
                                    "name": getattr(fr, "name", "") or "",
                                    "url": _mask_digits(fr_url)[:160],
                                    "has_menu_button": (fr.locator("button.btn-reference").count() > 0),
                                }
                            )
                        except Exception:
                            continue
                except Exception:
                    pass

                logger.warning("Recent Step2 did not reach menu page within timeout. state=%s", debug)

                # Optional: save artifacts if explicitly enabled (avoid leaking sensitive data by default)
                if save_artifacts:
                    try:
                        with open("/tmp/debug_recent_step2_timeout.html", "w", encoding="utf-8") as f:
                            f.write(page.content())
                    except Exception:
                        pass
                    try:
                        page.screenshot(path="/tmp/debug_recent_step2_timeout.png", full_page=True)
                    except Exception:
                        pass

                raise
            else:
                # ここまで来たら投票履歴一覧画面に到達している前提
                logger.info("Checking for history items (Today & Yesterday)...")
                target_days = [
                    ("Today", "label[for='refer-today']", 0),
                    ("Yesterday", "label[for='refer-before']", 1),
                ]

                for day_name, selector, day_offset in target_days:
                    logger.info("Switching to %s...", day_name)
                    page.click(selector)

                    try:
                        page.wait_for_selector("div.list-loading", state="visible", timeout=1000)
                        page.wait_for_selector("div.list-loading", state="hidden", timeout=10000)
                    except Exception:
                        page.wait_for_timeout(1000)

                    target_date = datetime.now() - timedelta(days=day_offset)
                    date_str = target_date.strftime("%Y%m%d")

                    rows = page.locator("table.table-status tbody tr")
                    count = rows.count()
                    logger.info("Found %d history items for %s.", count, day_name)

                    for i in range(count):
                        rows = page.locator("table.table-status tbody tr")
                        row = rows.nth(i)

                        try:
                            row_text = row.inner_text()
                            if "投票履歴がありません" in row_text:
                                logger.info("No history found for %s. Skipping.", day_name)
                                break
                        except Exception:
                            pass

                        try:
                            receipt_no = row.locator("td.receipt a").inner_text().strip()
                        except Exception:
                            logger.warning("Could not extract receipt no for row %d", i)
                            if "投票履歴がありません" not in row.inner_html():
                                html = row.inner_html()
                                logger.debug("Row HTML (truncated): %s", html[:1000])
                            continue

                        logger.info("Processing Receipt: %s", receipt_no)

                        try:
                            target_link = row.locator("td.receipt a")
                            target_link.scroll_into_view_if_needed()
                            target_link.click()
                        except Exception as e:
                            logger.warning("Failed to click receipt %s: %s", receipt_no, e)
                            continue

                        is_detail_loaded = False
                        try:
                            page.wait_for_selector(
                                "h1:has-text('投票履歴結果内容'), table.table-result", timeout=10000
                            )
                            is_detail_loaded = True
                        except Exception:
                            logger.warning(
                                "Failed to load detail view for %s. Attempting to recover...", receipt_no
                            )

                        if is_detail_loaded:
                            content = page.content()
                            try:
                                parsed = _parse_recent_detail_html(content, receipt_no, date_str, day_name == "Today")
                                all_parsed_data.extend(parsed)
                                logger.info("Extracted %d tickets.", len(parsed))
                            except Exception as e:
                                logger.warning("Error parsing detail for %s: %s", receipt_no, e)

                        try:
                            back_btn = page.locator("button:has-text('一覧に戻る')")
                            if back_btn.is_visible():
                                back_btn.click()
                            else:
                                close_btn = page.locator("button:has-text('閉じる')").last
                                if close_btn.is_visible():
                                    close_btn.click()

                            page.wait_for_selector("h1:has-text('投票履歴一覧')", timeout=5000)
                        except Exception:
                            if "投票履歴一覧" not in page.content():
                                logger.warning(
                                    "Could not confirm return to list for %s. Reloading page...", receipt_no
                                )
                                page.reload()
                                page.wait_for_selector("h1:has-text('投票履歴一覧')")

            finally:
                # デバッグ用: 画面確認のため終了を遅らせる
                keep_open_sec = _env_float("IPAT_DEBUG_KEEP_OPEN_SEC", 0.0)
                if keep_open_sec > 0:
                    try:
                        if debug_log:
                            logger.info("Keeping browser open for %.1fs for debugging...", keep_open_sec)
                        page.wait_for_timeout(int(keep_open_sec * 1000))
                    except Exception:
                        time.sleep(keep_open_sec)

                if trace_enabled:
                    try:
                        context.tracing.stop(path=trace_path)
                        if debug_log:
                            logger.info("Tracing saved. trace_path='%s'", trace_path)
                    except Exception as e:
                        logger.warning("Failed to stop/save tracing: %s", e)
                try:
                    context.close()
                finally:
                    browser.close()
        
    return all_parsed_data
