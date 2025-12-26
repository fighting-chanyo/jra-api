from fastapi import APIRouter, BackgroundTasks
from app.services.race_service import RaceService
from pydantic import BaseModel
import logging

router = APIRouter()

logger = logging.getLogger(__name__)

class ScheduleImportRequest(BaseModel):
    year: int
    month: int

@router.post("/races/import-schedule")
def import_schedule(req: ScheduleImportRequest, background_tasks: BackgroundTasks):
    service = RaceService()
    # 即時応答（重い処理はバックグラウンドで実行）
    logger.info("Scheduled import_schedule year=%s month=%s", req.year, req.month)
    background_tasks.add_task(service.import_schedule, req.year, req.month)
    return {"message": "Schedule import started.", "year": req.year, "month": req.month}

from typing import Optional
from datetime import date

@router.post("/races/update-results")
def update_results(background_tasks: BackgroundTasks, target_date: Optional[date] = None):
    service = RaceService()
    logger.info("Scheduled update_results target_date=%s", target_date)
    background_tasks.add_task(service.update_results, target_date)
    return {"message": "Result update started.", "target_date": target_date}
