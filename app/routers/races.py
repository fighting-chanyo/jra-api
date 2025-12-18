from fastapi import APIRouter, BackgroundTasks
from app.services.race_service import RaceService
from pydantic import BaseModel

router = APIRouter()

class ScheduleImportRequest(BaseModel):
    year: int
    month: int

@router.post("/races/import-schedule")
def import_schedule(req: ScheduleImportRequest, background_tasks: BackgroundTasks):
    service = RaceService()
    # Run in background as it might take time
    background_tasks.add_task(service.import_schedule, req.year, req.month)
    return {"message": "Schedule import started.", "year": req.year, "month": req.month}

from typing import Optional
from datetime import date

@router.post("/races/update-results")
def update_results(background_tasks: BackgroundTasks, target_date: Optional[date] = None):
    service = RaceService()
    # Run in background
    background_tasks.add_task(service.update_results, target_date)
    return {"message": "Result update started.", "target_date": target_date}
