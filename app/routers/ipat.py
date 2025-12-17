from fastapi import APIRouter, BackgroundTasks, status
from fastapi.responses import JSONResponse
from app.schemas import SyncIpatRequest
from app.services.ipat_service import sync_and_save_past_history, sync_and_save_recent_history

router = APIRouter()

@router.post("/sync/ipat")
def start_sync_ipat_data(req: SyncIpatRequest, background_tasks: BackgroundTasks):
    print(f"🔄 Sync request received for log_id: {req.log_id}, mode: {req.mode}")
    
    if req.mode == "recent":
        background_tasks.add_task(
            sync_and_save_recent_history,
            log_id=req.log_id,
            user_id=req.user_id,
            creds=req.ipat_auth
        )
    else:
        # Default to past history sync for "past" or any other value
        background_tasks.add_task(
            sync_and_save_past_history,
            log_id=req.log_id,
            user_id=req.user_id,
            creds=req.ipat_auth
        )

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"message": "Synchronization started.", "log_id": req.log_id}
    )
