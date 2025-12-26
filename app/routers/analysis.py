from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from app.services.gemini_service import GeminiService
from app.schemas import AnalysisResponse, AnalyzeQueueRequest
from app.services.analysis_service import process_analysis_queue
import logging

router = APIRouter()
gemini_service = GeminiService()

logger = logging.getLogger(__name__)

@router.post("/analyze/queue")
async def analyze_queue(request: AnalyzeQueueRequest, background_tasks: BackgroundTasks):
    logger.info("Scheduled analysis queue processing queueId=%s", request.queueId)
    background_tasks.add_task(process_analysis_queue, request.queueId)
    return {"message": "Analysis started", "queueId": request.queueId}

@router.post("/analyze/image", response_model=AnalysisResponse)
async def analyze_image(file: UploadFile = File(...)):
    if not file:
        raise HTTPException(status_code=400, detail="No file uploaded")
    
    try:
        logger.info("Analyze image request filename=%s content_type=%s", getattr(file, "filename", None), getattr(file, "content_type", None))
        content = await file.read()
        result = await gemini_service.analyze_image(content)
        if result:
            return AnalysisResponse(results=[result])
        else:
            return AnalysisResponse(results=[])
    except Exception as e:
        logger.exception("Analyze image failed")
        raise HTTPException(status_code=500, detail=str(e))
