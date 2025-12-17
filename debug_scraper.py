import requests
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def debug_race_list(date_str):
    url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date_str}"
    print(f"Fetching {url}...")
    
    resp = requests.get(url, headers=HEADERS)
    resp.encoding = 'EUC-JP'
    html = resp.text
    
    print(f"Status Code: {resp.status_code}")
    print(f"HTML Length: {len(html)}")
    
    # HTMLをファイルに保存
    filename = f"debug_race_list_{date_str}.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved HTML to {filename}")
    
    # キーワード検索
    keywords = ["RaceList_DataItem", "race_id=", "RaceList_DataList"]
    for kw in keywords:
        count = html.count(kw)
        print(f"Keyword '{kw}': found {count} times")

    # 正規表現テスト
    # 1. race_id抽出
    ids = re.findall(r'race_id=(\d+)', html)
    print(f"Found {len(ids)} race_ids: {ids[:5]}...")

if __name__ == "__main__":
    # 直近の開催日を指定
    debug_race_list("20251206")