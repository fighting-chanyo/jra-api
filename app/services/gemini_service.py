import os
import json
from google import genai
from google.genai import types
from app.schemas import AnalysisResponse, AnalysisResult, AnalyzedBetData
from typing import List
import base64

class GeminiService:
    def __init__(self):
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            print("Warning: GOOGLE_API_KEY not found in environment variables.")
            self.client = None
        else:
            self.client = genai.Client(api_key=api_key)
        
        # Using gemini-1.5-flash as default, but can be configured
        self.model_name = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")

    async def analyze_image(self, image_bytes: bytes) -> AnalysisResponse:
        if not self.client:
             return AnalysisResponse(results=[])

        prompt = """
        Analyze this image of a horse racing betting ticket or screen (JRA IPAT).
        Extract the betting information and return it in the following JSON structure.
        
        The response must be a valid JSON object with a "results" key, which is a list of objects.
        Each object should have:
        - "raw_text": string (OCR text for debugging)
        - "confidence": float (0.0 to 1.0)
        - "data": object with the following fields:
            - "date": string (YYYY-MM-DD). If year is missing, guess 2025.
            - "place": string (JRA Place Code: e.g., "05" for Tokyo, "06" for Nakayama, "09" for Hanshin, "08" for Kyoto).
            - "race_number": integer (1-12)
            - "bet_type": string (Enum: WIN, PLACE, BRACKET_QUINELLA, QUINELLA, QUINELLA_PLACE, EXACTA, TRIO, TRIFECTA)
            - "method": string (Enum: NORMAL, BOX, FORMATION, NAGASHI)
            - "selections": list of list of strings (e.g., [["01", "02"], ["03"]] for formation). For NORMAL/BOX, it's usually one list inside.
            - "axis": list of strings (for NAGASHI)
            - "partners": list of strings (for NAGASHI)
            - "multi": boolean (true if Multi/Multi-way)
            - "amount": integer (amount per point)

        Rules:
        - If a field is unclear or missing, set it to null.
        - For "selections" in FORMATION, ensure the order matches 1st, 2nd, 3rd place.
        - For NAGASHI, extract axis and partners correctly.
        - Convert Japanese text to the specified codes (e.g., "単勝" -> "WIN", "流し" -> "NAGASHI").
        """

        try:
            # google-genai client expects Part objects or specific content structure
            # For images, we can pass types.Part.from_bytes
            
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=[
                    prompt,
                    types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg")
                ],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            
            response_text = response.text
            # Parse JSON
            try:
                parsed_json = json.loads(response_text)
                return AnalysisResponse(**parsed_json)
            except json.JSONDecodeError:
                print(f"Failed to parse JSON from Gemini: {response_text}")
                return AnalysisResponse(results=[])
            except Exception as e:
                print(f"Validation error: {e}")
                # Try to return what we can or empty
                return AnalysisResponse(results=[])

        except Exception as e:
            print(f"Error calling Gemini API: {e}")
            # Return empty result on error
            return AnalysisResponse(results=[])
