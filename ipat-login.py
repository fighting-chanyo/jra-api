import os
import time
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# .envを読み込む
load_dotenv()

INET_ID = os.getenv("JRA_INET_ID")
SUBSCRIBER_NO = os.getenv("JRA_SUBSCRIBER_NO")
PIN = os.getenv("JRA_PIN")
PARS = os.getenv("JRA_PARS")

def main():
    print("🚀 JRA IPAT Login Script Started")
    
    with sync_playwright() as p:
        # headless=False にするとブラウザが立ち上がって見える（デバッグ用）
        # 本番(サーバー)では True にする
        browser = p.chromium.launch(headless=False, slow_mo=500)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            # 1. IPATトップページへアクセス
            print("Trying to access IPAT...")
            page.goto("https://www.ipat.jra.go.jp/")
            
            # 2. ログインボタンを探してクリック
            # ※IPATは時期によってUIが変わるため、テキスト検索が一番堅牢
            # スマホ版/PC版でリダイレクトされることがあるので注意
            if page.get_by_text("ネット投票ログイン").is_visible():
                 page.get_by_text("ネット投票ログイン").click()
            
            # 3. INET-ID 入力画面
            print("Entering INET-ID...")
            # name属性などで要素を特定 (実際のHTMLを見て調整が必要な場合あり)
            page.locator("input[name='inetid']").fill(INET_ID)
            page.keyboard.press("Enter") # または送信ボタンクリック
            
            # 4. 加入者番号・暗証番号・P-ARS 入力画面
            # 画面遷移を待つ
            page.wait_for_load_state("networkidle")
            
            print("Entering Credentials...")
            page.locator("input[name='i']").fill(SUBSCRIBER_NO) # 加入者番号
            page.locator("input[name='p']").fill(PIN)           # 暗証番号
            page.locator("input[name='r']").fill(PARS)          # P-ARS
            
            # ログイン実行
            # page.get_by_text("ログイン").click() でも良いが、Enterが確実な場合も
            page.locator(".loginBtn, input[type='submit']").click()

            # 5. ログイン成功判定
            # メニュー画面特有の要素があるかチェック
            page.wait_for_load_state("networkidle")
            
            if "投票メニュー" in page.title() or page.get_by_text("投票内容照会").is_visible():
                print("✅ Login SUCCESS!")
                
                # ここで「投票内容照会」をクリックしてデータを取る処理が続く...
                # page.get_by_text("投票内容照会").click()
                
                # デバッグ用に少し待機
                time.sleep(5)
            else:
                print("❌ Login FAILED. Maybe closed or wrong pass?")
                # 失敗時の画面をスクショ保存
                page.screenshot(path="login_error.png")

        except Exception as e:
            print(f"❌ Error occurred: {e}")
            page.screenshot(path="error_state.png")
        
        finally:
            browser.close()
            print("🏁 Script Finished")

if __name__ == "__main__":
    main()
