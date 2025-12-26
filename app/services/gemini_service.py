import os
import json
from google import genai
from google.genai import types
from app.schemas import AnalysisResult
from typing import List, Optional
import base64
import logging

logger = logging.getLogger(__name__)


class GeminiService:
    def __init__(self):
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            logger.warning("GOOGLE_API_KEY not found in environment variables.")
            self.client = None
        else:
            logger.info("GOOGLE_API_KEY found. Initializing Gemini client.")
            self.client = genai.Client(api_key=api_key)
        
        # Using gemini-1.5-flash as default, but can be configured
        self.model_name = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
        logger.info("Using Gemini model: %s", self.model_name)

    async def analyze_image(self, image_bytes: bytes) -> Optional[AnalysisResult]:
        logger.info("Starting image analysis.")
        if not self.client:
            logger.error("Gemini client is not initialized.")
            return None

        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")

        prompt = f"""
        あなたは「JRA IPAT の馬券画像（購入完了画面、購入内容確認、投票履歴、受付票など）」から投票内容を構造化する抽出器です。
        入力は画像1枚、出力は **JSONのみ**（前後に説明文やMarkdownを付けない）にしてください。

        現在日付: {today}（JST）

        # 目的
        画像内の情報を読み取り、可能な限り「ticketsテーブルに投入できる粒度」で抽出してください。

        # 出力JSONスキーマ（この形を厳守）
        {{
          "race": {{
            "date": "YYYY-MM-DD or null",
            "place": "開催場所名(例: 中山/東京/阪神/京都) or null",
            "race_number": 1-12 or null
          }},
          "tickets": [
            {{
              "receipt_unique_id": "string or null",

              "bet_type": "WIN|PLACE|BRACKET_QUINELLA|QUINELLA|QUINELLA_PLACE|EXACTA|TRIO|TRIFECTA",
              "buy_type": "NORMAL|BOX|FORMATION|NAGASHI",

              "content": {{
                "type": "同上(bet_typeと同一)",
                "method": "同上(buy_typeと同一)",
                "multi": true/false,

                "selections": "string[][]",
                "axis": "string[]",
                "partners": "string[]",
                "positions": "number[]"
              }},

              "amount_per_point": "integer or null",
              "total_points": "integer or null",
              "total_cost": "integer or null",

              "confidence": 0.0-1.0,
              "warnings": ["string", ...]
            }}
          ],
          "confidence": 0.0-1.0
        }}

        # 重要ルール
        ## A. 日付補完（race.date）
        - 年が不明なら「現在日付」に最も近くなる年に補完する。
        - 月日が不明なら race.date は「現在日付」を入れる。
        - 曜日だけ分かるなら「現在日付」に最も近いその曜日の日付にする。

        ## B. 場所・R
        - place は以下のコードに変換して出力する。
          札幌:01, 函館:02, 福島:03, 新潟:04, 東京:05, 中山:06, 中京:07, 京都:08, 阪神:09, 小倉:10
          (不明な場合は null)
        - race_number は 1〜12 の整数。

        ## C. bet_type（式別）変換
        単勝→WIN, 複勝→PLACE, 枠連→BRACKET_QUINELLA, 馬連→QUINELLA,
        ワイド→QUINELLA_PLACE, 馬単→EXACTA, 3連複→TRIO, 3連単→TRIFECTA

        ## D. buy_type（方式）判定
        - 「通常/ながし/流し/フォーメーション/BOX」等の文言から推定し、
          NORMAL / NAGASHI / FORMATION / BOX にマッピングする。
        - 判断できない場合は NORMAL を「推測で入れない」。null ではなく、buy_type自体を決められない場合は tickets自体の confidence を下げ、warningsに理由を書く。

        ## E. content の作り方（設計書準拠）
        ### 数値表記
        - 馬番・枠番は **必ず2桁ゼロ埋め文字列**で格納する（例: 1→"01", 10→"10"）。
        - 全角数字は半角に直す。区切り（- / → / , / ・ / 空白）に引きずられず正しく分解する。

        ### selections / axis / partners / positions
        - NORMAL/BOX:
          - selections の先頭要素だけを使う（例: 馬連 5-10 → selections=[["05","10"]]）
          - axis/partners/positions は空配列 []
        - FORMATION:
          - 着順（頭目）ごとに配列を分ける（例: 3連単 1着:5,6 /2着:5,6,10 /3着:1,2,3,10
            → selections=[["05","06"],["05","06","10"],["01","02","03","10"]])
          - axis/partners/positions は空配列 []
        - NAGASHI（流し）:
          - 軸馬は axis、相手は partners に入れる。selections は []
          - 「1着固定/2着固定/3着固定」が読み取れる場合のみ positions に入れる（例: 5を1着固定→positions=[1]）。
          - 「マルチ」や「着順不問」が明記される場合は multi=true、positions は []。

        ## F. 金額
        - amount_per_point（1点あたり）、total_points（点数）、total_cost（合計）を可能な限り抽出する。
        - もし total_cost と (amount_per_point * total_points) が矛盾する場合:
          - 3項目は読めた値をそのまま入れる
          - warnings に "cost_mismatch" を入れる
          - confidence を下げる

        ## G. 複数式別/複数行
        - 画像内に複数の式別・複数の買い目がある場合は tickets を複数要素に分ける。
        - 同一レース内の複数ticketでも race は共通でOK。

        ## H. 不明値
        - 分からないフィールドは null または []（配列フィールド）にする。
        - ただし **JSONのキーは省略しない**。

        # 最終チェック（必須）
        - bet_type は列挙値以外を出さない。
        - buy_type は列挙値以外を出さない。
        - content.type == bet_type、content.method == buy_type を満たす。
        - 馬番は必ず2桁文字列になっている。
        - 出力はJSONのみ。
        """

        try:
            # google-genai client expects Part objects or specific content structure
            # For images, we can pass types.Part.from_bytes
            logging.info("Sending request to Gemini API.")
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=[
                    prompt,
                    types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg")
                ],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            
            response_text = response.text
            logging.info(f"Received response from Gemini: {response_text}")
            # Parse JSON
            try:
                logging.info("Parsing JSON response.")
                parsed_json = json.loads(response_text)
                logging.info("JSON parsing successful. Validating schema.")
                response_obj = AnalysisResult(**parsed_json)
                logging.info("Schema validation successful.")
                return response_obj
            except json.JSONDecodeError:
                logging.error(f"Failed to parse JSON from Gemini: {response_text}")
                return None
            except Exception as e:
                logging.error(f"Validation error: {e}")
                # Try to return what we can or empty
                return None

        except Exception as e:
            logging.error(f"Error calling Gemini API: {e}", exc_info=True)
            # Return empty result on error
            return None
