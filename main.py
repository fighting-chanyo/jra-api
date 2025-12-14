from fastapi import FastAPI, BackgroundTasks, status
from fastapi.responses import JSONResponse
from app.schemas import SyncIpatRequest
from app.services.ipat_scraper import sync_and_save_past_history # sync_recent_historyは一旦除外

app = FastAPI()

@app.post("/api/sync/ipat")
def start_sync_ipat_data(req: SyncIpatRequest, background_tasks: BackgroundTasks):
    print(f"🔄 Sync request received for log_id: {req.log_id}")
    
    # 実際はsync_modeによって処理を分岐
    # if req.sync_mode == "past":
    background_tasks.add_task(
        sync_and_save_past_history,
        log_id=req.log_id,
        user_id=req.user_id,
        creds=req.ipat_auth
    )
    # else:
    #     # sync_recent_historyのバックグラウンド版も同様に実装
    #     pass

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"message": "Synchronization started.", "log_id": req.log_id}
    )