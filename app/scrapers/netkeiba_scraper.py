from typing import Optional, Dict, Any, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta, timezone
from app.constants import RACE_COURSE_MAP
import time
import random
import os
import logging

logger = logging.getLogger(__name__)

class NetkeibaScraper:
    BASE_URL = "https://race.netkeiba.com"

    def __init__(self):
        self._init_session()

        # 初回のみIPアドレスを確認してログに出す（外部依存を増やすのでデフォルトは無効）
        if os.getenv("NETKEIBA_CHECK_IP", "0") == "1":
            try:
                ip_resp = self.session.get("https://api.ipify.org", timeout=5)
                logger.info("Current Public IP: %s", ip_resp.text)
            except Exception as e:
                logger.warning("Failed to check public IP: %s", e)

    def _sleep_with_jitter(self, min_sec: float, max_sec: float) -> None:
        time.sleep(random.uniform(min_sec, max_sec))

    def _init_session(self) -> None:
        self.session = requests.Session()
        # Cloud Run等で環境変数HTTP(S)_PROXYが混入していると挙動が不安定になることがあるため無効化
        self.session.trust_env = False

        # ブラウザ風ヘッダは有効だが、Sec-* 系は整合性が取りづらくWAFで逆に怪しまれることがあるため最小限にする
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Referer": "https://race.netkeiba.com/",
            "Connection": "close",
        })

        # urllib3の自動リトライはSSLEOF等で意図せず短時間に連打になりやすいので無効化し、
        # アプリ側で指数バックオフ＋ジッター付きの手動リトライを行う
        retry = Retry(total=0, connect=0, read=0, redirect=0, status=0)
        adapter = HTTPAdapter(max_retries=retry, pool_connections=2, pool_maxsize=2)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get_html(self, url: str, encoding: str = None) -> Optional[str]:
        max_attempts = int(os.getenv("NETKEIBA_MAX_ATTEMPTS", "5"))
        base_sleep = float(os.getenv("NETKEIBA_BASE_SLEEP_SEC", "1.0"))
        timeout_connect = float(os.getenv("NETKEIBA_TIMEOUT_CONNECT_SEC", "5.0"))
        timeout_read = float(os.getenv("NETKEIBA_TIMEOUT_READ_SEC", "30.0"))

        # 1回目も軽くジッター（機械的な周期を避ける）
        self._sleep_with_jitter(0.5, 1.5)

        last_error: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                resp = self.session.get(url, timeout=(timeout_connect, timeout_read))

                # 429/5xx は再試行（WAF/レート制限想定）
                if resp.status_code in (429, 500, 502, 503, 504):
                    last_error = requests.HTTPError(f"HTTP {resp.status_code}")
                    raise requests.HTTPError(f"HTTP {resp.status_code}")

                resp.raise_for_status()
                if encoding:
                    return resp.content.decode(encoding, errors='replace')
                return resp.text

            except requests.exceptions.SSLError as e:
                # Cloud Run等でSSLEOFErrorが出る場合、接続プールをリセットして再試行
                last_error = e
                logger.warning(
                    "SSL error fetching %s (attempt %d/%d): %s",
                    url,
                    attempt,
                    max_attempts,
                    e,
                )
                self._init_session()

            except (requests.exceptions.ConnectionError, requests.HTTPError) as e:
                last_error = e
                logger.warning(
                    "Retryable error fetching %s (attempt %d/%d): %s",
                    url,
                    attempt,
                    max_attempts,
                    e,
                )

            except requests.exceptions.RequestException as e:
                # その他は基本再試行しない（タイムアウト等は上のConnectionErrorに入ることが多い）
                logger.error("Failed to fetch %s. Reason: %s", url, e)
                return None

            if attempt < max_attempts:
                backoff = base_sleep * (2 ** (attempt - 1))
                jitter = random.uniform(0.0, 1.0)
                time.sleep(min(backoff + jitter, 60.0))

        logger.error(
            "Failed to fetch %s after %d attempts. Last error: %s",
            url,
            max_attempts,
            last_error,
        )
        return None

    def _get_content(self, url: str) -> Optional[bytes]:
        """_get_html と同じリトライ/バックオフでレスポンスバイト列を取得する。"""
        max_attempts = int(os.getenv("NETKEIBA_MAX_ATTEMPTS", "5"))
        base_sleep = float(os.getenv("NETKEIBA_BASE_SLEEP_SEC", "1.0"))
        timeout_connect = float(os.getenv("NETKEIBA_TIMEOUT_CONNECT_SEC", "5.0"))
        timeout_read = float(os.getenv("NETKEIBA_TIMEOUT_READ_SEC", "30.0"))

        self._sleep_with_jitter(0.5, 1.5)

        last_error: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                resp = self.session.get(url, timeout=(timeout_connect, timeout_read))

                if resp.status_code in (429, 500, 502, 503, 504):
                    last_error = requests.HTTPError(f"HTTP {resp.status_code}")
                    raise requests.HTTPError(f"HTTP {resp.status_code}")

                resp.raise_for_status()
                return resp.content

            except requests.exceptions.SSLError as e:
                last_error = e
                logger.warning(
                    "SSL error fetching %s (attempt %d/%d): %s",
                    url,
                    attempt,
                    max_attempts,
                    e,
                )
                self._init_session()

            except (requests.exceptions.ConnectionError, requests.HTTPError) as e:
                last_error = e
                logger.warning(
                    "Retryable error fetching %s (attempt %d/%d): %s",
                    url,
                    attempt,
                    max_attempts,
                    e,
                )

            except requests.exceptions.RequestException as e:
                logger.error("Failed to fetch %s. Reason: %s", url, e)
                return None

            if attempt < max_attempts:
                backoff = base_sleep * (2 ** (attempt - 1))
                jitter = random.uniform(0.0, 1.0)
                time.sleep(min(backoff + jitter, 60.0))

        logger.error(
            "Failed to fetch %s after %d attempts. Last error: %s",
            url,
            max_attempts,
            last_error,
        )
        return None

    def scrape_monthly_schedule(self, year: int, month: int):
        """
        指定された年月の開催スケジュールを取得する
        """
        url = f"{self.BASE_URL}/top/calendar.html?year={year}&month={month}"
        
        html_content = self._get_html(url, encoding='euc-jp')
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'html.parser')

        race_dates = []
        
        # パターン1: リンクから kaisai_date を探す
        links = soup.find_all("a", href=True)
        for link in links:
            href = link.get("href")
            if "kaisai_date=" in href:
                match = re.search(r'kaisai_date=(\d{8})', href)
                if match:
                    d = match.group(1)
                    if d not in race_dates:
                        race_dates.append(d)

        # パターン2: ユーザー提供のHTML構造から日付を抽出する
        kaisai_boxes = soup.select("div.RaceKaisaiBox.HaveData")
        
        for box in kaisai_boxes:
            day_span = box.select_one("span.Day")
            if day_span:
                try:
                    day_text = day_span.text.strip()
                    day_match = re.search(r'\d+', day_text)
                    if day_match:
                        day = int(day_match.group(0))
                        date_str = f"{year}{month:02d}{day:02d}"
                        if date_str not in race_dates:
                            race_dates.append(date_str)
                except ValueError:
                    continue

        race_dates.sort()

        logger.info("Extracted %d race dates for %04d-%02d: %s", len(race_dates), year, month, race_dates)

        # カレンダーHTMLが一部欠けている等で「連続開催日の1日だけ」が抜けることがあるため、
        # 1日ギャップ(例: 2/8, 2/10 の間の 2/9)だけは補完チェックする。
        # 連続開催でない通常の平日は補完しない（余計なアクセス増を避ける）。
        prefetched_races: Dict[str, List[Dict[str, Any]]] = {}
        try:
            parsed_dates = [datetime.strptime(d, "%Y%m%d").date() for d in race_dates]
            for i in range(len(parsed_dates) - 1):
                delta_days = (parsed_dates[i + 1] - parsed_dates[i]).days
                if delta_days != 2:
                    continue

                missing_date = parsed_dates[i] + timedelta(days=1)
                if missing_date.year != year or missing_date.month != month:
                    continue

                missing_str = missing_date.strftime("%Y%m%d")
                if missing_str in race_dates:
                    continue

                logger.warning(
                    "Detected a 1-day gap between %s and %s; probing missing date %s",
                    race_dates[i],
                    race_dates[i + 1],
                    missing_str,
                )
                races = self._scrape_race_list(missing_str)
                if races:
                    logger.info("Gap-fill: found %d races for %s; including it", len(races), missing_str)
                    prefetched_races[missing_str] = races
                    race_dates.append(missing_str)
        except Exception as e:
            logger.warning("Gap-fill check skipped due to error: %s", e)

        race_dates.sort()

        all_races = []
        for date_str in race_dates:
            logger.info("Fetching race list for %s...", date_str)
            races = prefetched_races.get(date_str)
            if races is None:
                races = self._scrape_race_list(date_str)
            all_races.extend(races)
            # ループ内でも待機（_get_html内のsleepと合わせて長めになる）
            time.sleep(1)
            
        return all_races

    def _scrape_race_list(self, date_str: str):
        """
        特定の日付の全レース情報を取得する
        """
        url = f"{self.BASE_URL}/top/race_list_sub.html?kaisai_date={date_str}"
        
        html_content = self._get_html(url, encoding='utf-8')
        if not html_content:
            logger.error("Failed to fetch race list for %s.", date_str)
            return []

        if "race_id=" not in html_content:
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        race_items = soup.select(".RaceList_DataItem")
        
        races = []
        for item in race_items:
            try:
                r_div = item.select_one(".Race_Num")
                if not r_div: continue
                r_num_str = r_div.text.strip().replace('R', '')
                race_number = int(r_num_str)
                
                name_div = item.select_one(".ItemTitle")
                race_name = name_div.text.strip() if name_div else "Unknown"
                
                time_div = item.select_one(".RaceList_Itemtime")
                post_time_str = time_div.text.strip() if time_div else None
                post_time = None
                if post_time_str:
                    hm = post_time_str.split(':')
                    if len(hm) == 2:
                        year = int(date_str[:4])
                        month = int(date_str[4:6])
                        day = int(date_str[6:8])
                        post_time = datetime(year, month, day, int(hm[0]), int(hm[1]), tzinfo=timezone(timedelta(hours=9)))
                
                link = item.select_one("a")
                if not link: continue
                href = link.get("href")
                match = re.search(r'race_id=(\d+)', href)
                if not match: continue
                
                external_id = match.group(1)
                nk_place_code = external_id[4:6]
                place_code = nk_place_code 
                
                races.append({
                    "date": date_str,
                    "place_code": place_code,
                    "race_number": race_number,
                    "name": race_name,
                    "post_time": post_time,
                    "external_id": external_id
                })
            except Exception as e:
                logger.warning("Error parsing race item: %s", e)
                continue
                
        return races

    def scrape_race_result(self, external_id: str) -> Optional[Dict[str, Any]]:
        """
        レース結果と払戻金を取得する
        """
        url = f"{self.BASE_URL}/race/result.html?race_id={external_id}"

        try:
            content = self._get_content(url)
            if not content:
                return None

            try:
                html_content = content.decode('utf-8')
            except UnicodeDecodeError:
                html_content = content.decode('euc-jp', errors='replace')

            soup = BeautifulSoup(html_content, 'html.parser')

            # --- 1. 着順の取得 ---
            result_table = soup.find("table", class_="RaceTable01")
            if not result_table:
                logger.info("Result table not found for %s (not finalized yet?)", external_id)
                return None

            rows = result_table.find_all("tr", class_=re.compile("HorseList"))
            if len(rows) < 3:
                logger.info("Not enough result rows found for %s (not finalized yet?)", external_id)
                return None
            
            try:
                result_1st = rows[0].select_one("td.Result_Num + td + td div").text.strip()
                result_2nd = rows[1].select_one("td.Result_Num + td + td div").text.strip()
                result_3rd = rows[2].select_one("td.Result_Num + td + td div").text.strip()
            except (AttributeError, IndexError):
                logger.warning("Could not parse 1st-3rd place horse numbers for %s", external_id)
                return None

            # 終了直後など「結果枠はあるが中身がモザイク/未確定」の場合がある。
            # その場合は誤って確定扱いしないよう、最低限の数値検証を行う。
            if not (result_1st.isdigit() and result_2nd.isdigit() and result_3rd.isdigit()):
                logger.info(
                    "Result numbers are not finalized for %s (masked/non-digit): %s/%s/%s",
                    external_id,
                    result_1st,
                    result_2nd,
                    result_3rd,
                )
                return None
            
            # --- 2. 払戻金の取得 ---
            payout_data = {}
            payout_box = soup.find("div", class_="Result_Pay_Back")
            if not payout_box:
                # 払戻枠が出ていない場合は未確定扱い
                logger.info("Payout box not found for %s (not finalized yet?)", external_id)
                return None

            payout_tables = payout_box.find_all("table")
            
            bet_type_map = {
                "Tansho": "WIN",
                "Fukusho": "PLACE",
                "Wakuren": "BRACKET_QUINELLA",
                "Umaren": "QUINELLA",
                "Wide": "QUINELLA_PLACE",
                "Umatan": "EXACTA",
                "Fuku3": "TRIO",
                "Tan3": "TRIFECTA"
            }

            for table in payout_tables:
                for tr in table.find_all("tr"):
                    th = tr.find("th")
                    if not th: continue
                    
                    tr_class = tr.get("class", [""])[0]
                    bet_type_key = bet_type_map.get(tr_class)

                    if not bet_type_key:
                        continue
                    
                    result_td = tr.find("td", class_="Result")
                    payout_td = tr.find("td", class_="Payout")

                    if not result_td or not payout_td:
                        continue

                    payout_texts = [p.strip() for p in payout_td.decode_contents().split('<br>')]
                    payout_monies = [int(re.sub(r'\D', '', p)) for p in payout_texts if re.search(r'\d', p)]

                    horse_groups = []
                    def get_numbers_from_tags(tags):
                        nums = []
                        for tag in tags:
                            num_text = tag.text.strip()
                            if num_text.isdigit():
                                nums.append(int(num_text))
                        return nums

                    if result_td.find("ul"):
                        groups = result_td.find_all("ul")
                        for group in groups:
                            numbers = get_numbers_from_tags(group.find_all("li"))
                            if numbers:
                                horse_groups.append(numbers)
                    else:
                        numbers = get_numbers_from_tags(result_td.find_all("div"))
                        for num in numbers:
                            horse_groups.append([num])

                    payout_list = []
                    if len(horse_groups) == len(payout_monies):
                        for i, horses in enumerate(horse_groups):
                            try:
                                payout_list.append({
                                    "horse": horses,
                                    "money": payout_monies[i]
                                })
                            except (ValueError, IndexError):
                                continue
                    
                    if payout_list:
                        payout_data[bet_type_key] = payout_list

            # 払戻が空の場合は未確定、もしくはHTML変更でパースできていない可能性がある。
            # いずれにせよ誤って確定扱いしないよう、ここで弾く。
            if not payout_data:
                logger.info("Payout data empty for %s (not finalized or parse failed)", external_id)
                return None

            return {
                "result_1st": result_1st,
                "result_2nd": result_2nd,
                "result_3rd": result_3rd,
                "payout_data": payout_data,
            }

        except requests.exceptions.RequestException as e:
            logger.warning("Error scraping result for %s (Request failed): %s", external_id, e)
            return None
        except Exception as e:
            logger.exception("Error scraping result for %s", external_id)
            return None
