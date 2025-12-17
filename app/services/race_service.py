from datetime import date, datetime, timedelta
import json
import time
from app.services.netkeiba_scraper import NetkeibaScraper
from app.services.supabase_client import get_supabase_client
from app.services.judgment_logic import JudgmentLogic
from app.schemas import Race, PayoutData, Ticket

class RaceService:
    def __init__(self):
        self.scraper = NetkeibaScraper()
        self.supabase = get_supabase_client()

    def import_schedule(self, year: int, month: int):
        """
        指定年月のレーススケジュールを取り込み、DBに保存する
        """
        print(f"📅 Importing schedule for {year}-{month}...")
        try:
            races_data = self.scraper.scrape_monthly_schedule(year, month)
            print(f"DEBUG: Scraped {len(races_data)} races.")
            
            # DBから既存データを取得して比較するための準備
            # 月の範囲を計算
            start_date = date(year, month, 1)
            if month == 12:
                end_date = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = date(year, month + 1, 1) - timedelta(days=1)
            
            # 既存データを取得
            # id, name, post_time, external_id があれば判定可能
            existing_races_resp = self.supabase.table("races") \
                .select("id, name, post_time, external_id") \
                .gte("date", start_date.isoformat()) \
                .lte("date", end_date.isoformat()) \
                .execute()
            
            existing_races_map = {r['id']: r for r in existing_races_resp.data}
            
            db_records = []
            skipped_count = 0

            for r in races_data:
                # ID生成: YYYYMMDD + PlaceCode(2) + RaceNo(2)
                race_id = f"{r['date']}{r['place_code']}{str(r['race_number']).zfill(2)}"
                
                # dateオブジェクトへの変換
                race_date = datetime.strptime(r['date'], "%Y%m%d").date()
                
                record = {
                    "id": race_id,
                    "date": race_date.isoformat(),
                    "place_code": r['place_code'],
                    "race_number": r['race_number'],
                    "name": r['name'],
                    "post_time": r['post_time'].isoformat() if r['post_time'] else None,
                    "external_id": r['external_id'],
                }

                # 既存データチェック
                if race_id in existing_races_map:
                    existing = existing_races_map[race_id]
                    # 必須フィールドが全て埋まっているかチェック
                    # id, date, place_code, race_number は not null なので、
                    # name, post_time, external_id をチェック
                    is_complete = (
                        existing.get('name') is not None and
                        existing.get('post_time') is not None and
                        existing.get('external_id') is not None
                    )
                    
                    if is_complete:
                        # 既に完全なデータがあるのでスキップ
                        skipped_count += 1
                        continue

                db_records.append(record)
            
            print(f"DEBUG: Skipped {skipped_count} complete records. Upserting {len(db_records)} records.")
            
            if db_records:
                # バッチでUpsert (50件ずつ分割して送信)
                print(f"   Upserting {len(db_records)} races...")
                
                batch_size = 50
                for i in range(0, len(db_records), batch_size):
                    batch = db_records[i:i + batch_size]
                    try:
                        self.supabase.table("races").upsert(batch).execute()
                        print(f"   Upserted batch {i // batch_size + 1} ({len(batch)} records)")
                        time.sleep(1) # レート制限回避
                    except Exception as e:
                        print(f"ERROR upserting batch {i // batch_size + 1}: {e}")
            
            return len(db_records)
        except Exception as e:
            print(f"ERROR in import_schedule: {e}")
            import traceback
            traceback.print_exc()
            return 0

    def update_results_for_today(self):
        """
        当日のレース結果を更新し、的中判定を行う
        """
        today = date.today()
        # テスト用に日付を固定したい場合はここで調整
        # today = date(2023, 12, 17) 
        
        print(f"🏁 Updating results for {today}...")
        
        # 1. DBから当日のレースを取得 (status != 'FINISHED' かつ post_time が過去)
        # post_time <= now - 10min
        check_time = datetime.now() - timedelta(minutes=10)
        
        # Supabaseクエリ
        # date = today AND status != 'FINISHED'
        # post_time filter is harder in Supabase simple client if not using raw sql or range
        # とりあえず当日全件取得してフィルタリング
        res = self.supabase.table("races").select("*").eq("date", today.isoformat()).neq("status", "FINISHED").execute()
        races = res.data
        
        if not races:
            print("   No pending races found for today.")
            return {"processed": 0, "hits": 0}

        processed_count = 0
        total_hits = 0

        for race in races:
            # 発走時刻チェック
            if race.get("post_time"):
                post_time = datetime.fromisoformat(race["post_time"])
                # タイムゾーン情報がない場合はnaive比較になるので注意
                # DBがtimestamptzならUTCで返ってくることが多い
                # ここでは簡易的に比較（エラーが出たら修正）
                if post_time.tzinfo:
                    if post_time > datetime.now(post_time.tzinfo):
                        continue # まだ発走していない
                else:
                    if post_time > datetime.now():
                        continue

            external_id = race.get("external_id")
            if not external_id:
                continue

            print(f"   Checking result for Race {race['id']} (Ext: {external_id})...")
            
            # 2. 結果スクレイピング
            result_data = self.scraper.scrape_race_result(external_id)
            if not result_data:
                print("      -> Not finalized yet.")
                continue

            # 3. DB更新 (Races)
            update_payload = {
                "result_1st": result_data["result_1st"],
                "result_2nd": result_data["result_2nd"],
                "result_3rd": result_data["result_3rd"],
                "payout_data": result_data["payout_data"],
                "status": "FINISHED"
            }
            self.supabase.table("races").update(update_payload).eq("id", race["id"]).execute()
            processed_count += 1

            # 4. 的中判定
            hits = self._process_hit_detection(race["id"], result_data)
            total_hits += hits

        return {"processed": processed_count, "hits": total_hits}

    def _process_hit_detection(self, race_id: str, result_data: dict):
        """
        特定のレースに対するチケットの的中判定を行う
        """
        # 対象レースのPENDINGチケットを取得
        res = self.supabase.table("tickets").select("*").eq("race_id", race_id).eq("status", "PENDING").execute()
        tickets = res.data
        
        if not tickets:
            return 0

        print(f"      Processing {len(tickets)} tickets for Race {race_id}...")
        
        hit_count = 0
        payout_data_obj = PayoutData(**result_data["payout_data"])
        
        # 1着〜3着の馬番 (int)
        try:
            r1 = int(result_data["result_1st"])
            r2 = int(result_data["result_2nd"])
            r3 = int(result_data["result_3rd"])
        except (ValueError, TypeError):
            print("      ⚠️ Error parsing result horse numbers.")
            return 0

        for t_dict in tickets:
            # Ticketモデルに変換
            ticket = Ticket(**t_dict)
            
            status, payout = JudgmentLogic.judge_ticket(ticket, r1, r2, r3, payout_data_obj)
            
            if status == "HIT":
                hit_count += 1
                print(f"         🎉 HIT! Ticket {ticket.id}: {payout} yen")
            
            # チケット更新
            self.supabase.table("tickets").update({
                "status": status,
                "payout": payout
            }).eq("id", ticket.id).execute()
            
        return hit_count
