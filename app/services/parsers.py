import csv
import io
import re
import logging
from bs4 import BeautifulSoup
from app.constants import BET_TYPE_MAP

logger = logging.getLogger(__name__)

def parse_jra_csv(csv_path):
    results = []
    try:
        with open(csv_path, "r", encoding="shift_jis") as f:
            # デリミタをカンマに変更してCSVリーダーを作成
            reader = csv.reader(f, delimiter=',')
            all_rows = list(reader)

        header_index = -1
        # ヘッダー行を特定する（先頭列が "日付" であるかで判断）
        for i, row in enumerate(all_rows):
            if row and row[0].strip() == "日付":
                header_index = i
                break
        
        if header_index == -1:
            logger.warning("CSV Header not found")
            return []

        # ヘッダーとデータ行を分割
        header = [h.strip() for h in all_rows[header_index]]
        data_rows = all_rows[header_index + 1:]
        
        col_map = {name: i for i, name in enumerate(header)}
        logger.info("CSV Header mapped: %s", list(col_map.keys()))

        current_receipt_no = None
        current_line_counter = 0

        for row in data_rows:
            # 【修正】行のいずれかのセルに「合計」が含まれていたらスキップする
            if not row or len(row) < len(header) or any("合計" in str(cell) for cell in row):
                continue
            
            receipt_no = row[col_map["受付番号"]]
            if receipt_no != current_receipt_no:
                current_receipt_no = receipt_no
                current_line_counter = 1
            else:
                current_line_counter += 1

            try:
                # 購入金額 (単価と合計)
                amount_str = row[col_map["購入金額"]]
                amount_per_point, total_cost = 0, 0
                if '／' in amount_str:
                    parts = amount_str.split('／')
                    amount_per_point = int(parts[0])
                    total_cost = int(parts[1])
                else:
                    amount_per_point = int(amount_str)
                    total_cost = int(amount_str)

                # 購入点数
                total_points = 0
                if amount_per_point > 0:
                    total_points = total_cost // amount_per_point

                # 払戻金
                payout_str = row[col_map["払戻金額"]].replace(',', '')
                payout = int(payout_str) if payout_str.isdigit() else 0
                
                # ステータス
                status = "PENDING" # DB保存時はPENDINGをデフォルトに
                if "的中" in row[col_map["的中／返還"]]:
                    status = "WIN"
                elif payout == 0 and amount_per_point > 0:
                    status = "LOSE"

                shikibetsu_str = row[col_map["式別"]]
                kumiban_str = row[col_map["馬／組番"]]
                normalized_shikibetsu_str = shikibetsu_str.replace('3', '３')

                # 式別コードの特定
                bet_type_code = "unknown"
                for jp, en in BET_TYPE_MAP.items():
                    if jp in normalized_shikibetsu_str:
                        bet_type_code = en
                        break
                
                method, multi, axis, partners, selections, positions = "NORMAL", False, [], [], [], []

                if "ＢＯＸ" in shikibetsu_str or "ボックス" in shikibetsu_str:
                    method = "BOX"
                    selections = [[x.strip() for x in kumiban_str.split('；') if x.strip()]]
                elif "フォーメーション" in shikibetsu_str:
                    method = "FORMATION"
                    selections = [[x.strip() for x in part.split('；') if x.strip()] for part in kumiban_str.split('／')]
                elif "ながし" in shikibetsu_str:
                    method = "NAGASHI"
                    if "マルチ" in shikibetsu_str: multi = True
                    
                    # Parse positions
                    if not multi:
                        # Extract numbers before "着"
                        match = re.search(r'([123１２３・]+)着', shikibetsu_str)
                        if match:
                            pos_str = match.group(1)
                            for p in pos_str.split('・'):
                                p = p.strip()
                                if p.isdigit():
                                    positions.append(int(p))
                                elif p in ['１', '２', '３']:
                                    positions.append(int(p.translate(str.maketrans('１２３', '123'))))

                    parts = kumiban_str.split('／')
                    
                    # Logic for assigning axis/partners
                    if len(parts) == 3 and positions and not multi:
                        # 3-part format with fixed positions (e.g. 3連単1・3着ながし)
                        # parts[0] -> 1st, parts[1] -> 2nd, parts[2] -> 3rd
                        for i, part in enumerate(parts):
                            horses = [x.strip() for x in part.split('；') if x.strip()]
                            current_pos = i + 1
                            if current_pos in positions:
                                axis.extend(horses)
                            else:
                                partners.extend(horses)
                    else:
                        # Standard Axis / Partners format
                        if len(parts) >= 2:
                            axis = [x.strip() for x in parts[0].split('；') if x.strip()]
                            partners = [x.strip() for x in parts[1].split('；') if x.strip()]
                else:
                    selections = [re.findall(r'\d+', kumiban_str)]

                # DB保存用に構造化して返す
                ticket_data = {
                    "raw": {
                        "receipt_no": receipt_no,
                        "line_no": current_line_counter,
                        "race_date_str": row[col_map["日付"]], # YYYYMMDD
                        "race_place": row[col_map["場名"]],
                        "race_number_str": row[col_map["レース"]], # "R"なし
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
                        "total_cost": total_cost,
                        "payout": payout,
                        "status": status,
                        "source": "IPAT_CSV",
                        "mode": "REAL"
                    }
                }
                results.append(ticket_data)
            except (IndexError, KeyError, ValueError) as e:
                logger.warning("CSV Row Parse Error: %s | Row: %s", e, row)

    except Exception as e:
        logger.exception("CSV Parse Error")
        
    return results

def parse_past_detail_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []

    # 日付 (変更なし)
    date_header = soup.select_one('.headingBlock.type2 h2')
    if date_header:
        date_text = date_header.get_text(strip=True)
        date_match = re.search(r'(\d+)年(\d+)月\s*(\d+)日', date_text)
        if date_match:
            race_date = f"{date_match.group(1)}-{date_match.group(2).zfill(2)}-{date_match.group(3).zfill(2)}"
        else:
            race_date = "0000-00-00"
    else:
        race_date = "0000-00-00"

    entries = soup.select('.voteData > ul > li')

    for entry in entries:
        header = entry.select_one('h4')
        if not header: continue

        place_name = header.select_one('.jouname').get_text(strip=True) if header.select_one('.jouname') else "Unknown"
        race_no_raw = header.select_one('.raceno').get_text(strip=True) if header.select_one('.raceno') else "0"
        vote_kind_text = header.select_one('.voteKind').get_text(strip=True) if header.select_one('.voteKind') else ""
        
        # 金額
        buy_money_elem = header.select_one('.hbuyMoney span:nth-of-type(2)')
        amount = int(buy_money_elem.get_text(strip=True).replace('円', '').replace(',', '')) if buy_money_elem else 0
        
        # 払戻
        back_money_elem = header.select_one('.hbackMoney span:nth-of-type(2)')
        payout = 0
        status = "LOSE"
        if back_money_elem:
            payout_text = back_money_elem.get_text(strip=True).replace('円', '').replace(',', '')
            if payout_text.isdigit():
                payout = int(payout_text)
                if payout > 0:
                    status = "WIN"

        umaban_info = entry.select_one('.umabanInfo')
        bet_type_code, buy_type_method, is_multi = analyze_vote_kind(vote_kind_text)
        
        # 設計書に準拠したcontentオブジェクトを生成
        content_json = {
            "type": bet_type_code,
            "method": buy_type_method,
            "multi": is_multi,
            "axis": [],
            "partners": [],
            "selections": [],
            "positions": []
        }
        
        if umaban_info:
            blocks = umaban_info.select('.buyInfo > div')
            
            if buy_type_method == "NAGASHI":
                axis_list = []
                partners_list = []
                positions_list = []

                for block in blocks:
                    prefix_elem = block.select_one('.prefix')
                    prefix_text = prefix_elem.get_text(strip=True) if prefix_elem else ""
                    nums = [p.get_text(strip=True) for p in block.select('.umabanBlock p')]
                    
                    if "軸" in prefix_text:
                        current_positions = []
                        if not is_multi:
                            # Regex to find 1-3 (half or full width) followed by 着 or 頭目
                            pos_match = re.search(r"([123１２３・]+)(?:着|頭目)", prefix_text)
                            if pos_match:
                                pos_str = pos_match.group(1)
                                pos_map = {"1": 1, "2": 2, "3": 3, "１": 1, "２": 2, "３": 3}
                                for char in pos_str:
                                    if char in pos_map:
                                        current_positions.append(pos_map[char])
                        
                        if not current_positions:
                            axis_list.extend(nums)
                        else:
                            for pos in current_positions:
                                axis_list.extend(nums)
                                positions_list.extend([pos] * len(nums))
                    else:
                        partners_list.extend(nums)
                
                content_json["axis"] = axis_list
                content_json["partners"] = partners_list
                content_json["positions"] = positions_list

            elif buy_type_method == "BOX":
                # ボックスで選択した馬番をselectionsに二次元配列として格納
                nums = [p.get_text(strip=True) for p in umaban_info.select('.umabanBlock p')]
                content_json["selections"] = [nums]

            elif buy_type_method == "FORMATION":
                # フォーメーションの各選択肢をselectionsに格納
                selections = []
                for block in blocks:
                    selections.append([p.get_text(strip=True) for p in block.select('.umabanBlock p')])
                content_json["selections"] = selections

            else: # NORMAL
                # 通常投票の組み合わせをselectionsに格納
                nums = [p.get_text(strip=True) for p in umaban_info.select('.umabanBlock p')]
                content_json["selections"] = [nums]

        ticket = {
            "race_place": place_name,
            "race_number": race_no_raw + "R",
            "race_date": race_date,
            "content": content_json,
            "amount": amount,
            "payout": payout,
            "status": status,
            "mode": "REAL"
        }
        results.append(ticket)

    return results

def analyze_vote_kind(text):
    bet_type = "unknown"
    for jp, en in BET_TYPE_MAP.items():
        if jp in text:
            bet_type = en
            break
            
    # 投票方式を大文字のコードで返すように修正
    buy_type = "NORMAL"
    is_multi = False
    
    if "ながし" in text:
        buy_type = "NAGASHI"
        if "マルチ" in text: is_multi = True
    elif "ボックス" in text:
        buy_type = "BOX"
    elif "フォーメーション" in text:
        buy_type = "FORMATION"
        
    return bet_type, buy_type, is_multi