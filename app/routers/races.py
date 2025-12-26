from fastapi import APIRouter, BackgroundTasks, Body
from app.services.race_service import RaceService
from pydantic import BaseModel
import logging
import calendar
from datetime import date, datetime, timedelta, timezone
from typing import Optional

router = APIRouter()

logger = logging.getLogger(__name__)

class ScheduleImportRequest(BaseModel):
    year: int | None = None
    month: int | None = None


def _default_schedule_year_month(today_jst: date) -> tuple[int, int]:
    last_day = calendar.monthrange(today_jst.year, today_jst.month)[1]
    end_of_month = date(today_jst.year, today_jst.month, last_day)
    remaining_days = (end_of_month - today_jst).days

    # 月末まで1週間未満なら翌月をデフォルトにする
    if remaining_days < 7:
        if today_jst.month == 12:
            return today_jst.year + 1, 1
        return today_jst.year, today_jst.month + 1

    return today_jst.year, today_jst.month

@router.post("/races/import-schedule")
def import_schedule(
    background_tasks: BackgroundTasks,
    req: ScheduleImportRequest = Body(default_factory=ScheduleImportRequest),
):
    service = RaceService()
    # 即時応答（重い処理はバックグラウンドで実行）
    jst = timezone(timedelta(hours=9))
    today_jst = datetime.now(jst).date()

    year = getattr(req, "year", None)
    month = getattr(req, "month", None)
    if not year or not month:
        year, month = _default_schedule_year_month(today_jst)

    logger.info("Scheduled import_schedule year=%s month=%s (today_jst=%s)", year, month, today_jst)
    background_tasks.add_task(service.import_schedule, year, month)
    return {"message": "Schedule import started.", "year": year, "month": month}

@router.post("/races/update-results")
def update_results(background_tasks: BackgroundTasks, target_date: Optional[date] = None):
    service = RaceService()
    logger.info("Scheduled update_results target_date=%s", target_date)
    background_tasks.add_task(service.update_results, target_date)
    return {"message": "Result update started.", "target_date": target_date}
