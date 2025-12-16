import os
import json
import traceback
from app.schemas import IpatAuth
from app.services.ipat_scraper import _scrape_past_history_csv
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

def run_test():
    """
    スクレイピングとCSVパース処理をテストします。
    """
    print("🧪 Starting Scraper Test...")

    # 環境変数から認証情報を取得 (JRA_ プレフィックスに修正)
    inet_id = os.getenv("JRA_INET_ID")
    subscriber_no = os.getenv("JRA_SUBSCRIBER_NO")
    pin = os.getenv("JRA_PIN")
    p_ars = os.getenv("JRA_PARS")

    if not all([inet_id, subscriber_no, pin, p_ars]):
        print("❌ Error: Missing credentials in environment variables.")
        print("Please check your '.env' file and ensure JRA_INET_ID, JRA_SUBSCRIBER_NO, JRA_PIN, and JRA_PARS are set.")
        return

    # テスト用のリクエストデータを作成
    test_creds = IpatAuth(
        inet_id=inet_id,
        subscriber_number=subscriber_no,
        password=pin,
        pars_number=p_ars
    )

    print("- Target: _scrape_past_history_csv (CSV Download & Parse)")
    print(f"- User: {subscriber_no}")

    try:
        # メインの処理を実行
        result = _scrape_past_history_csv(test_creds)

        # 結果を出力
        print("\n" + "="*20 + " TEST RESULT " + "="*20)
        if result:
            output_filename = "test_output.json"
            with open(output_filename, "w", encoding="utf-8") as f:
                # result は dict のリストなのでそのままダンプ可能
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Success! Found {len(result)} tickets.")
            print(f"📄 Full parsed data has been written to '{output_filename}'.")
            
            # サンプル表示
            if len(result) > 0:
                print("--- Sample Data (First ticket) ---")
                print(json.dumps(result[0], indent=2, ensure_ascii=False))
                print("------------------------------------")

        else:
            print("⚠️ No tickets found (result is empty list).")

    except Exception as e:
        print(f"❌ Test Failed: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_test()
