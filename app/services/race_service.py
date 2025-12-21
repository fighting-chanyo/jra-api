from datetime import date, datetime, timedelta, timezone
import json
import time
from app.scrapers.netkeiba_scraper import NetkeibaScraper
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

    def update_results(self, target_date: date = None):
        """
        指定日のレース結果を更新し、的中判定を行う
        target_date: 指定がない場合は当日(date.today())
        """
        target_date = target_date or date.today()
        
        print(f"INFO: Starting result update process for target_date={target_date}...")
        
        # 1. DBから当日のレースを取得 (status != 'FINISHED')
        # post_time のフィルタリングはPython側で行う
        
        # Supabaseクエリ
        res = self.supabase.table("races").select("*").eq("date", target_date.isoformat()).neq("status", "FINISHED").execute()
        races = res.data
        
        if not races:
            print(f"INFO: No pending races found for {target_date}.")
            return {"processed": 0, "hits": 0}

        processed_count = 0
        total_hits = 0
        
        # 現在時刻をUTCで取得
        now_utc = datetime.now(timezone.utc)

        for race in races:
            # 発走時刻チェック
            if race.get("post_time"):
                try:
                    # ISOフォーマット文字列をパース
                    post_time = datetime.fromisoformat(race["post_time"])
                    
                    # タイムゾーン情報の有無を確認してUTCに統一
                    if post_time.tzinfo is None:
                        # タイムゾーン情報がない場合、DBがUTCで保存していると仮定してUTCを付与
                        # もしJSTで保存されているなら timezone(timedelta(hours=9)) を付与
                        # Supabaseのtimestamptzは通常UTCで返る
                        post_time = post_time.replace(tzinfo=timezone.utc)
                    else:
                        # タイムゾーン情報がある場合はUTCに変換
                        post_time = post_time.astimezone(timezone.utc)
                    
                    # 現在時刻と比較 (発走時刻 > 現在時刻 ならスキップ)
                    if post_time > now_utc:
                        # print(f"   Skipping Race {race['id']}: Post time {post_time} is in the future (Now: {now_utc})")
                        continue
                        
                except ValueError as e:
                    print(f"   ⚠️ Error parsing post_time for Race {race['id']}: {e}")
                    continue

            external_id = race.get("external_id")
            if not external_id:
                continue

            # print(f"   Checking result for Race {race['id']} (Ext: {external_id})...")
            
            # 2. 結果スクレイピング
            result_data = self.scraper.scrape_race_result(external_id)
            if not result_data:
                # print("      -> Not finalized yet.")
                continue

            # print(f"DEBUG: Scraped result data for {race['id']}: {result_data}")

            # 3. DB更新 (Races)
            update_payload = {
                "result_1st": result_data["result_1st"],
                "result_2nd": result_data["result_2nd"],
                "result_3rd": result_data["result_3rd"],
                "payout_data": result_data["payout_data"],
                "status": "FINISHED"
            }
            # print(f"DEBUG: Updating race {race['id']} with payload: {update_payload}")
            
            try:
                self.supabase.table("races").update(update_payload).eq("id", race["id"]).execute()
                print(f"INFO: Result found for race {race['id']}. Updated DB.")
            except Exception as e:
                print(f"ERROR updating race {race['id']}: {e}")

            processed_count += 1

            # 4. 的中判定
            hits = self._process_hit_detection(race["id"], result_data)
            total_hits += hits

        print(f"INFO: Result update process completed. Processed: {processed_count}, Hits: {total_hits}")
        return {"processed": processed_count, "hits": total_hits}

    def judge_existing_races(self, race_ids: list[str]):
        """
        指定されたレースIDリストのうち、既に結果が確定しているものについて
        即座に判定処理を行う
        """
        if not race_ids:
            return

        print(f"🔍 Checking for finished races among {len(race_ids)} IDs...")
        
        # Supabaseの `in` フィルタを使って一括取得
        # status='FINISHED' のものだけ取得
        try:
            res = self.supabase.table("races") \
                .select("*") \
                .in_("id", race_ids) \
                .eq("status", "FINISHED") \
                .execute()
            
            finished_races = res.data
            if not finished_races:
                print("   No finished races found in the provided list.")
                return

            print(f"   Found {len(finished_races)} finished races. Running judgment...")

            for race in finished_races:
                # DBのレコードから result_data を再構築
                # update_results で使っている形式に合わせる
                result_data = {
                    "result_1st": race.get("result_1st"),
                    "result_2nd": race.get("result_2nd"),
                    "result_3rd": race.get("result_3rd"),
                    "payout_data": race.get("payout_data")
                }
                
                # 必須データが欠けていないか簡易チェック
                if not (result_data["result_1st"] and result_data["payout_data"]):
                    print(f"   ⚠️ Race {race['id']} is marked FINISHED but lacks result data.")
                    continue

                # 判定処理を実行
                hits = self._process_hit_detection(race["id"], result_data)
                print(f"   Race {race['id']}: Processed judgment. Hits: {hits}")

        except Exception as e:
            print(f"ERROR in judge_existing_races: {e}")

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
            
            judged_status, payout = JudgmentLogic.judge_ticket(ticket, r1, r2, r3, payout_data_obj)
            
            # フロントエンドの仕様に合わせて "HIT" を "WIN" に変更
            status_to_update = "WIN" if judged_status == "HIT" else judged_status
            
            if status_to_update == "WIN":
                hit_count += 1
                print(f"         🎉 WIN! Ticket {ticket.id}: {payout} yen")
            
            # チケット更新
            self.supabase.table("tickets").update({
                "status": status_to_update,
                "payout": payout
            }).eq("id", ticket.id).execute()
            
        return hit_count
