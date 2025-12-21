from typing import Optional, Dict, Any, List
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta, timezone
from app.constants import RACE_COURSE_MAP
import time

class NetkeibaScraper:
    BASE_URL = "https://race.netkeiba.com"
    # User-Agentを設定してブラウザからのアクセスに見せかける
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    def scrape_monthly_schedule(self, year: int, month: int):
        """
        指定された年月の開催スケジュールを取得する
        """
        url = f"{self.BASE_URL}/top/calendar.html?year={year}&month={month}"
        # print(f"DEBUG: Accessing calendar URL: {url}")
        
        try:
            # headersを指定してリクエスト、タイムアウトを設定
            resp = requests.get(url, headers=self.HEADERS, timeout=10)
            resp.raise_for_status() # HTTPエラーがあれば例外を発生させる
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Failed to fetch calendar page for {year}-{month}: {e}")
            return []
        
        # resp.encoding = 'EUC-JP' # 削除
        
        # print(f"DEBUG: Response Status: {resp.status_code}")
        
        # バイナリデータから明示的にデコード
        html_content = resp.content.decode('euc-jp', errors='replace')
        soup = BeautifulSoup(html_content, 'html.parser')

        race_dates = []
        
        # パターン1: リンクから kaisai_date を探す (念のため残す)
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
        # <div class="RaceKaisaiBox HaveData"><p><span class="Day">6</span></p>...</div>
        kaisai_boxes = soup.select("div.RaceKaisaiBox.HaveData")
        # print(f"DEBUG: Found {len(kaisai_boxes)} boxes with class 'RaceKaisaiBox HaveData'")
        
        for box in kaisai_boxes:
            day_span = box.select_one("span.Day")
            if day_span:
                try:
                    day_text = day_span.text.strip()
                    # 数字以外が含まれている可能性を考慮して抽出
                    day_match = re.search(r'\d+', day_text)
                    if day_match:
                        day = int(day_match.group(0))
                        # YYYYMMDD 形式にする
                        date_str = f"{year}{month:02d}{day:02d}"
                        if date_str not in race_dates:
                            race_dates.append(date_str)
                except ValueError:
                    continue

        race_dates.sort()
        # print(f"DEBUG: Found {len(race_dates)} race dates: {race_dates}")

        all_races = []
        for date_str in race_dates:
            print(f"Fetching race list for {date_str}...")
            races = self._scrape_race_list(date_str)
            # print(f"DEBUG: Found {len(races)} races for {date_str}")
            all_races.extend(races)
            # サーバーへ負荷をかけすぎないように、リクエスト間に1秒の待機時間を設ける
            time.sleep(1)
            
        return all_races

    def _scrape_race_list(self, date_str: str):
        """
        特定の日付の全レース情報を取得する
        """
        # race_list.html はガワだけで、中身は race_list_sub.html で取得している可能性が高い
        url = f"{self.BASE_URL}/top/race_list_sub.html?kaisai_date={date_str}"
        # print(f"DEBUG: Accessing race list URL: {url}")
        
        try:
            # headersを指定、タイムアウトを設定
            resp = requests.get(url, headers=self.HEADERS, timeout=10)
            resp.raise_for_status() # 4xx or 5xx エラーの場合に例外を発生させる
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Failed to fetch race list for {date_str}. Reason: {e}")
            return [] # エラーが発生した場合は空のリストを返す
        
        # resp.encoding = 'EUC-JP' # 削除
        
        # バイナリデータから明示的にデコード
        # race_list_sub.html はUTF-8の可能性が高い (文字化け "2罩恰" -> "2歳" から推測)
        html_content = resp.content.decode('utf-8', errors='replace')

        # --- DEBUG: HTMLの内容確認 ---
        # race_idが含まれているかチェック
        if "race_id=" not in html_content:
            # print(f"DEBUG: 'race_id=' NOT FOUND in response text for {date_str}")
            # 念のため元のURLも試す（あるいは別のパラメータが必要か）
            return []
        else:
            # print(f"DEBUG: 'race_id=' found in response text.")
            pass
            
        # race_id を抽出
        # <tr class="RaceList_DataList"> ... <a href="../race/result.html?race_id=202306050911&rf=race_list">
        # あるいは <a href="/race/shutuba.html?race_id=...">
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # レース一覧の行を取得
        # Netkeibaの構造は複雑だが、RaceList_DataItem などを探す
        race_items = soup.select(".RaceList_DataItem")
        # print(f"DEBUG: Found {len(race_items)} race items")
        
        races = []
        for item in race_items:
            try:
                # R番号
                r_div = item.select_one(".Race_Num")
                if not r_div: continue
                r_num_str = r_div.text.strip().replace('R', '')
                race_number = int(r_num_str)
                
                # レース名
                # name_div = item.select_one(".Race_Name") # 旧セレクタ
                name_div = item.select_one(".ItemTitle") # ご提示のHTML構造に合わせて修正
                race_name = name_div.text.strip() if name_div else "Unknown"
                
                # 発走時刻
                time_div = item.select_one(".RaceList_Itemtime")
                post_time_str = time_div.text.strip() if time_div else None
                post_time = None
                if post_time_str:
                    # date_str (YYYYMMDD) + HH:MM
                    # datetime object
                    hm = post_time_str.split(':')
                    if len(hm) == 2:
                        year = int(date_str[:4])
                        month = int(date_str[4:6])
                        day = int(date_str[6:8])
                        # JSTのタイムゾーン情報を付与
                        post_time = datetime(year, month, day, int(hm[0]), int(hm[1]), tzinfo=timezone(timedelta(hours=9)))
                
                # 場所コード
                # URLから場所コードを推測するのは難しいが、race_idから取れる
                # race_id: YYYY PP DD RR NN (PP: Place Code)
                # Netkeiba race_id: 2023 06 05 09 11
                # 2023: Year
                # 06: Place (Nakayama?) -> Need mapping
                # 05: Kai (5th meeting)
                # 09: Day (9th day)
                # 11: Race Num
                
                # リンクからrace_idを取得
                link = item.select_one("a")
                if not link: continue
                href = link.get("href")
                match = re.search(r'race_id=(\d+)', href)
                if not match: continue
                
                external_id = match.group(1)
                
                # Netkeiba Place Code -> JRA Place Code
                # Netkeiba: 01:Sapporo, 02:Hakodate, 03:Fukushima, 04:Niigata, 05:Tokyo, 06:Nakayama, 07:Chukyo, 08:Kyoto, 09:Hanshin, 10:Kokura
                # JRA: Same?
                # Let's assume same for now, or use mapping if needed.
                # app/constants.py might have RACE_COURSE_MAP
                
                nk_place_code = external_id[4:6]
                # マッピングが必要ならここで変換
                # 今回はそのまま使う（JRAコードと一致していることが多い）
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
                print(f"Error parsing race item: {e}")
                continue
                
        return races

    def scrape_race_result(self, external_id: str) -> Optional[Dict[str, Any]]:
        """
        レース結果と払戻金を取得する
        """
        url = f"{self.BASE_URL}/race/result.html?race_id={external_id}"
        # print(f"DEBUG: Accessing result URL: {url}")
        
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=10)
            resp.raise_for_status() # 4xx or 5xx エラーの場合に例外を発生させる
            
            # エンコーディング処理の改善
            # まずUTF-8を試行し、失敗したらEUC-JP (Netkeibaは混在しているため)
            try:
                html_content = resp.content.decode('utf-8')
            except UnicodeDecodeError:
                html_content = resp.content.decode('euc-jp', errors='replace')

            soup = BeautifulSoup(html_content, 'html.parser')

            # --- 1. 着順の取得 ---
            result_table = soup.find("table", class_="RaceTable01")
            if not result_table:
                print(f"   -> Result table not found for {external_id}.")
                return None

            rows = result_table.find_all("tr", class_=re.compile("HorseList"))
            if len(rows) < 3:
                print(f"   -> Not enough result rows found for {external_id}.")
                return None
            
            try:
                result_1st = rows[0].select_one("td.Result_Num + td + td div").text.strip()
                result_2nd = rows[1].select_one("td.Result_Num + td + td div").text.strip()
                result_3rd = rows[2].select_one("td.Result_Num + td + td div").text.strip()
            except (AttributeError, IndexError):
                print(f"   -> Could not parse 1st-3rd place horse numbers for {external_id}")
                return None
            
            # --- 2. 払戻金の取得 ---
            payout_data = {}
            payout_box = soup.find("div", class_="Result_Pay_Back")
            if not payout_box:
                print(f"   -> Payout box not found for {external_id}.")
                # 着順だけでも返す
                return {
                    "result_1st": result_1st,
                    "result_2nd": result_2nd,
                    "result_3rd": result_3rd,
                    "payout_data": {} # 空のデータを返す
                }

            # 各払い戻しテーブルを処理
            payout_tables = payout_box.find_all("table")
            
            # 券種名とクラス名のマッピング (設計書のキーに合わせる)
            bet_type_map = {
                "Tansho": "WIN",
                "Fukusho": "PLACE",
                "Wakuren": "BRACKET_QUINELLA", # JRA投票にないので無視しても良い
                "Umaren": "QUINELLA",
                "Wide": "QUINELLA_PLACE",
                "Umatan": "EXACTA",
                "Fuku3": "TRIO", # 3連複
                "Tan3": "TRIFECTA"    # 3連単
            }

            for table in payout_tables:
                for tr in table.find_all("tr"):
                    # th から券種名を取得
                    th = tr.find("th")
                    if not th: continue
                    
                    # class名から券種キーを取得
                    tr_class = tr.get("class", [""])[0]
                    bet_type_key = bet_type_map.get(tr_class)

                    if not bet_type_key:
                        continue
                    
                    result_td = tr.find("td", class_="Result")
                    payout_td = tr.find("td", class_="Payout")

                    if not result_td or not payout_td:
                        continue

                    # 払い戻し金額の取得 (先に取得)
                    # "520円" や "160円<br>190円" のようになっている
                    payout_texts = [p.strip() for p in payout_td.decode_contents().split('<br>')]
                    payout_monies = [int(re.sub(r'\D', '', p)) for p in payout_texts if re.search(r'\d', p)]

                    # 馬番リストの取得
                    horse_groups = []
                    # <span> や <li> から馬番を抽出するヘルパー
                    def get_numbers_from_tags(tags):
                        nums = []
                        for tag in tags:
                            num_text = tag.text.strip()
                            if num_text.isdigit():
                                nums.append(int(num_text))
                        return nums

                    if result_td.find("ul"): # 馬連、ワイド、3連系など
                        groups = result_td.find_all("ul")
                        for group in groups:
                            numbers = get_numbers_from_tags(group.find_all("li"))
                            if numbers:
                                horse_groups.append(numbers)
                    else: # 単勝、複勝
                        numbers = get_numbers_from_tags(result_td.find_all("div"))
                        # 単勝・複勝は各馬番が1つの結果に対応する
                        for num in numbers:
                            horse_groups.append([num])

                    # データ整形
                    payout_list = []
                    # 同着などで horse_groups と payout_monies の数が一致することを確認
                    if len(horse_groups) == len(payout_monies):
                        for i, horses in enumerate(horse_groups):
                            try:
                                payout_list.append({
                                    "horse": horses,
                                    "money": payout_monies[i]
                                })
                            except (ValueError, IndexError):
                                print(f"  WARN: Could not parse payout entry for {bet_type_key}")
                                continue
                    
                    if payout_list:
                        payout_data[bet_type_key] = payout_list


            return {
                "result_1st": result_1st,
                "result_2nd": result_2nd,
                "result_3rd": result_3rd,
                "payout_data": payout_data,
            }

        except requests.exceptions.RequestException as e:
            print(f"ERROR scraping result for {external_id} (Request failed): {e}")
            return None
        except Exception as e:
            print(f"ERROR scraping result for {external_id}: {e}")
            import traceback
            traceback.print_exc()
            return None
