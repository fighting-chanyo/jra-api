from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from app.services.gemini_service import GeminiService
from app.schemas import AnalysisResponse, AnalyzeQueueRequest
from app.services.analysis_service import process_analysis_queue

router = APIRouter()
gemini_service = GeminiService()

@router.post("/analyze/queue")
async def analyze_queue(request: AnalyzeQueueRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(process_analysis_queue, request.queueId)
    return {"message": "Analysis started", "queueId": request.queueId}

@router.post("/analyze/image", response_model=AnalysisResponse)
async def analyze_image(file: UploadFile = File(...)):
    if not file:
        raise HTTPException(status_code=400, detail="No file uploaded")
    
    try:
        content = await file.read()
        result = await gemini_service.analyze_image(content)
        if result:
            return AnalysisResponse(results=[result])
        else:
            return AnalysisResponse(results=[])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
