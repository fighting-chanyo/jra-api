import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from app.constants import RACE_COURSE_MAP

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
        print(f"DEBUG: Accessing calendar URL: {url}")
        
        # headersを指定してリクエスト
        resp = requests.get(url, headers=self.HEADERS)
        # resp.encoding = 'EUC-JP' # 削除
        
        print(f"DEBUG: Response Status: {resp.status_code}")
        
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
        print(f"DEBUG: Found {len(kaisai_boxes)} boxes with class 'RaceKaisaiBox HaveData'")
        
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
        print(f"DEBUG: Found {len(race_dates)} race dates: {race_dates}")

        all_races = []
        for date_str in race_dates:
            print(f"Fetching race list for {date_str}...")
            races = self._scrape_race_list(date_str)
            print(f"DEBUG: Found {len(races)} races for {date_str}")
            all_races.extend(races)
            
        return all_races

    def _scrape_race_list(self, date_str: str):
        """
        特定の日付の全レース情報を取得する
        """
        # race_list.html はガワだけで、中身は race_list_sub.html で取得している可能性が高い
        url = f"{self.BASE_URL}/top/race_list_sub.html?kaisai_date={date_str}"
        print(f"DEBUG: Accessing race list URL: {url}")
        
        # headersを指定
        resp = requests.get(url, headers=self.HEADERS)
        # resp.encoding = 'EUC-JP' # 削除
        
        # バイナリデータから明示的にデコード
        # race_list_sub.html はUTF-8の可能性が高い (文字化け "2罩恰" -> "2歳" から推測)
        html_content = resp.content.decode('utf-8', errors='replace')

        # --- DEBUG: HTMLの内容確認 ---
        # race_idが含まれているかチェック
        if "race_id=" not in html_content:
            print(f"DEBUG: 'race_id=' NOT FOUND in response text for {date_str}")
            # 念のため元のURLも試す（あるいは別のパラメータが必要か）
            return []
        else:
            print(f"DEBUG: 'race_id=' found in response text.")
        # ---------------------------

        races = []
        
        # 正規表現でレース情報を一括抽出する
        # race_list_sub.html の構造に合わせて抽出
        
        # race_idを含むaタグを探す
        # <a href="../race/shutuba.html?race_id=202506050601...
        # 注意: subページでは相対パスや構造が少し違うかもしれない
        
        # まずは race_id を全て抽出してみる
        # setを使って重複排除
        found_ids = set(re.findall(r'race_id=(\d+)', html_content))
        
        # 各IDについて、周辺情報を探すのは難しい（HTML全体から探す必要があるため）
        # ここでは、BeautifulSoupを使って構造解析を試みる（subページなら構造が単純かもしれない）
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # li.RaceList_DataItem を探す
        items = soup.find_all("li", class_="RaceList_DataItem")
        print(f"DEBUG: Found {len(items)} 'li.RaceList_DataItem' elements")
        
        for li in items:
            # 場所名の特定
            # 親要素を遡って場所名を探す
            place_code = None
            dl = li.find_parent("dl", class_="RaceList_DataList")
            if dl:
                title_tag = dl.find("p", class_="RaceList_DataTitle")
                if title_tag:
                    place_name_full = title_tag.text.strip()
                    for k, v in RACE_COURSE_MAP.items():
                        if k in place_name_full:
                            place_code = v
                            break
            
            link = li.find("a")
            if not link:
                continue
            
            href = link.get("href")
            race_id_match = re.search(r'race_id=(\d+)', href)
            if not race_id_match:
                continue
            external_id = race_id_match.group(1)
            
            # 場所コードが取れていない場合、IDから補完
            if not place_code and len(external_id) == 12:
                place_code = external_id[4:6]
            
            if not place_code:
                continue

            # レース番号
            race_no_tag = li.find("div", class_="Race_Num")
            if race_no_tag:
                race_no_text = race_no_tag.text.strip()
                try:
                    race_no = int(re.search(r'\d+', race_no_text).group(0))
                except:
                    race_no = int(external_id[-2:])
            else:
                race_no = int(external_id[-2:])
            
            # レース名
            race_name_tag = li.find("span", class_="ItemTitle")
            race_name = race_name_tag.text.strip() if race_name_tag else ""
            
            # 発走時刻
            time_tag = li.find("span", class_="RaceList_Itemtime")
            post_time_str = time_tag.text.strip() if time_tag else "00:00"
            
            try:
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                hour, minute = map(int, post_time_str.split(':'))
                post_time = datetime(year, month, day, hour, minute)
            except:
                post_time = None
                
            races.append({
                "date": date_str,
                "place_code": place_code,
                "race_number": race_no,
                "name": race_name,
                "post_time": post_time,
                "external_id": external_id
            })

        return races

    def scrape_race_result(self, external_id: str):
        """
        レース結果と払戻金を取得する
        """
        url = f"{self.BASE_URL}/race/result.html?race_id={external_id}"
        # headersを指定
        resp = requests.get(url, headers=self.HEADERS)
        # resp.encoding = 'EUC-JP' # 削除
        
        html_content = resp.content.decode('euc-jp', errors='replace')
        soup = BeautifulSoup(html_content, 'html.parser')

        # 確定チェック (1着の馬番があるか)
        # table.RaceTable01 tr (1行目はヘッダー)
        result_rows = soup.select("table.RaceTable01 tr.HorseList")
        if not result_rows:
            return None # まだ結果がない
        
        # 1着, 2着, 3着の馬番を取得
        # 着順は tr の中の td:nth-child(1) だが、確定後は着順が入る
        # Netkeibaは着順通りに並んでいるはず
        
        results = []
        for row in result_rows[:3]: # 上位3頭
            # 馬番は td:nth-child(3) (枠, 馬番, ...)
            # 構造が変わる可能性があるのでclassで探したいが、Netkeibaはclassが少ない
            # 通常: 着順, 枠, 馬番, 馬名...
            tds = row.find_all("td")
            if len(tds) < 3:
                continue
            
            try:
                horse_no = int(tds[2].text.strip())
                results.append(horse_no)
            except ValueError:
                continue
        
        if len(results) < 3:
            # 3着まで確定していない、あるいは同着などで複雑な場合
            # とりあえず3頭取れなければスキップ扱いにしてもよいが、
            # 運用上は確定していれば取れるはず
            pass

        # 払戻金の取得
        # table.PayBackTable
        payout_data = {}
        
        # Netkeibaの払戻テーブルは2つある場合がある（単勝〜馬連、ワイド〜3連単）
        # class="PayBackTable" を全て取得
        payback_tables = soup.select("table.PayBackTable")
        
        for table in payback_tables:
            rows = table.find_all("tr")
            for row in rows:
                th = row.find("th")
                if not th: continue
                type_name = th.text.strip() # 単勝, 複勝, etc.
                
                # 該当するキーを探す
                key = None
                if "単勝" in type_name: key = "WIN"
                elif "複勝" in type_name: key = "PLACE"
                elif "枠連" in type_name: key = "BRACKET_QUINELLA"
                elif "馬連" in type_name: key = "QUINELLA"
                elif "ワイド" in type_name: key = "QUINELLA_PLACE"
                elif "馬単" in type_name: key = "EXACTA"
                elif "3連複" in type_name or "３連複" in type_name: key = "TRIO"
                elif "3連単" in type_name or "３連単" in type_name: key = "TRIFECTA"
                
                if not key: continue
                
                # 組み合わせと配当
                # td.Result (組み合わせ), td.Payout (配当)
                # 複勝やワイドは複数行ある場合があるが、Netkeibaは1つのtdの中にbrで区切られていることが多い
                # あるいは tr が分かれていることは少ない（Netkeibaは1つのセルに押し込むスタイル）
                
                result_td = row.find("td", class_="Result")
                payout_td = row.find("td", class_="Payout")
                
                if not result_td or not payout_td: continue
                
                # brで分割
                # get_text(separator='|') を使う
                results_text = result_td.get_text(separator='|').split('|')
                payouts_text = payout_td.get_text(separator='|').split('|')
                
                items = []
                for r_txt, p_txt in zip(results_text, payouts_text):
                    # r_txt: "10" or "10 - 11" or "10 - 11 - 12"
                    # p_txt: "1,200" -> 1200
                    
                    try:
                        money = int(p_txt.replace(',', '').replace('円', ''))
                        # 馬番の抽出
                        # - で区切られている、あるいは枠連などはそのまま
                        # 正規表現で数字を抽出
                        horses = [int(x) for x in re.findall(r'\d+', r_txt)]
                        items.append({"horse": horses, "money": money})
                    except ValueError:
                        continue
                
                payout_data[key] = items

        return {
            "result_1st": str(results[0]) if len(results) > 0 else None,
            "result_2nd": str(results[1]) if len(results) > 1 else None,
            "result_3rd": str(results[2]) if len(results) > 2 else None,
            "payout_data": payout_data
        }
