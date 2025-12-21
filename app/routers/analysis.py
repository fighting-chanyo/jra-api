from fastapi import APIRouter, UploadFile, File, HTTPException
from app.services.gemini_service import GeminiService
from app.schemas import AnalysisResponse

router = APIRouter()
gemini_service = GeminiService()

@router.post("/analyze/image", response_model=AnalysisResponse)
async def analyze_image(file: UploadFile = File(...)):
    if not file:
        raise HTTPException(status_code=400, detail="No file uploaded")
    
    try:
        content = await file.read()
        result = await gemini_service.analyze_image(content)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
