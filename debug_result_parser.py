
import requests
from bs4 import BeautifulSoup
import json

def test_scrape_result(race_id):
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    print(f"Fetching {url}")
    resp = requests.get(url)
    resp.encoding = 'EUC-JP' # Netkeiba is usually EUC-JP, but sometimes UTF-8. Let's check.
    
    # Check encoding
    # print(resp.text[:500])
    
    soup = BeautifulSoup(resp.content, 'html.parser')
    
    # 1. Extract 1st, 2nd, 3rd place
    # Table class "RaceResultTable"
    result_table = soup.find("table", id="All_Result_Table")
    if not result_table:
        print("Result table not found")
        return

    trs = result_table.find_all("tr", class_="HorseList")
    print(f"Found {len(trs)} horses")
    
    places = {}
    for tr in trs:
        try:
            rank_div = tr.select_one("div.Rank")
            if not rank_div: continue
            rank = rank_div.text.strip()
            
            umaban_td = tr.select_one("td.Umaban")
            if not umaban_td: continue
            umaban = umaban_td.text.strip()
            
            if rank in ["1", "2", "3"]:
                places[rank] = umaban
                print(f"Rank {rank}: Horse {umaban}")
        except Exception as e:
            print(f"Error parsing row: {e}")

    # 2. Extract Payout
    # Table class "PayBackTable" usually inside "Full_PayBack" div?
    # Or class "PayBack"
    
    payout_div = soup.select_one("dl.PayBack") # Sometimes it's dl?
    # Actually, looking at source, it might be different.
    # Let's look for tables with class "PayBackTable"
    
    payback_tables = soup.select("table.PayBackTable")
    print(f"Found {len(payback_tables)} payback tables")
    
    payout_data = {}
    
    for table in payback_tables:
        rows = table.find_all("tr")
        for row in rows:
            th = row.find("th")
            if not th: continue
            label = th.text.strip() # 単勝, 複勝, etc.
            
            # The structure is usually:
            # <th>単勝</th>
            # <td><ul><li>...</li></ul></td> (Result)
            # <td><ul><li>...</li></ul></td> (Money)
            
            tds = row.find_all("td")
            if len(tds) < 2: continue
            
            result_td = tds[0]
            money_td = tds[1]
            
            # Extract results (horse numbers)
            # Sometimes multiple results (e.g. Fukusho)
            # They might be in <div class="PayBack_Y"> or just text separated by br
            # Netkeiba usually uses <ul class="PayBack_Y"><li>...</li></ul>? No.
            # Let's inspect the content.
            
            # Handle multiple entries (e.g. Fukusho)
            # Usually separated by <br> or in separate divs?
            # Actually, Netkeiba often puts multiple winning horses in one cell, separated by something.
            # Or multiple <tr>s? No, usually one <tr> for Fukusho with multiple lines.
            
            # Let's try to parse the text content carefully.
            # Using 'ul' if exists?
            
            results = []
            moneys = []
            
            # Check for ul/li
            if result_td.find("ul"):
                results = [li.text.strip() for li in result_td.find_all("li")]
                moneys = [li.text.strip().replace(',', '').replace('円', '') for li in money_td.find_all("li")]
            else:
                # Maybe separated by <br>
                # get_text(separator='|')
                r_text = result_td.get_text(separator='|').strip()
                m_text = money_td.get_text(separator='|').strip()
                
                results = [x.strip() for x in r_text.split('|') if x.strip()]
                moneys = [x.strip().replace(',', '').replace('円', '') for x in m_text.split('|') if x.strip()]
            
            print(f"Label: {label}")
            print(f"  Results: {results}")
            print(f"  Moneys: {moneys}")

if __name__ == "__main__":
    # Use a likely valid ID
    test_scrape_result("202406010101")
