import os
import json
from app.schemas import IpatLoginRequest
from app.services.ipat_scraper import sync_past_history
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
    test_creds = IpatLoginRequest(
        inet_id=inet_id,
        subscriber_no=subscriber_no,
        pin=pin,
        p_ars=p_ars
    )

    print("- Target: sync_past_history (CSV Download & Parse)")
    print(f"- User: {subscriber_no}")

    try:
        # メインの処理を実行
        result = sync_past_history(test_creds)

        # 結果を出力
        print("\n" + "="*20 + " TEST RESULT " + "="*20)
        if result and result.get("data"):
            # --- ここから追加 ---
            output_filename = "test_output.json"
            with open(output_filename, "w", encoding="utf-8") as f:
                json.dump(result['data'], f, indent=2, ensure_ascii=False)
            
            print(f"✅ Success! Found {len(result['data'])} tickets.")
            print(f"📄 Full parsed data has been written to '{output_filename}'.")
            # --- ここまで追加 ---

            print("--- Sample Data (First ticket) ---")
            print(json.dumps(result['data'][0], indent=2, ensure_ascii=False))
            print("------------------------------------")
            print("--- Sample Data (Last ticket) ---")
            print(json.dumps(result['data'][-1], indent=2, ensure_ascii=False))
            print("------------------------------------")
        elif result and "data" in result and not result["data"]:
             print(f"✅ Success! Process finished but no tickets were found.")
             print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            print("⚠️ Test finished but received an unexpected result.")
            print(result)


    except Exception as e:
        print("\n" + "="*20 + " TEST FAILED " + "="*20)
        print(f"❌ An exception occurred: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "="*53)


if __name__ == "__main__":
    run_test()