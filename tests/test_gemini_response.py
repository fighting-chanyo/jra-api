import pytest
from app.schemas import AnalysisResponse, AnalyzedBetData

def test_analysis_response_with_null_multi():
    """
    Test that AnalysisResponse can handle 'multi': null in the JSON data.
    This reproduces the error reported by the user.
    """
    json_data = {
        "results": [
            {
                "raw_text": "Sample Text",
                "confidence": 0.98,
                "data": {
                    "date": "2025-12-21",
                    "place": "09",
                    "race_number": 11,
                    "bet_type": "QUINELLA_PLACE",
                    "method": "FORMATION",
                    "selections": [["03"], ["02", "08"]],
                    "axis": None,
                    "partners": None,
                    "multi": None,  # This caused the error
                    "amount": 2500
                }
            }
        ]
    }
    
    response = AnalysisResponse(**json_data)
    assert len(response.results) == 1
    assert response.results[0].data.multi is None
    assert response.results[0].data.race_number == 11

def test_analysis_response_with_missing_multi():
    """
    Test that AnalysisResponse can handle missing 'multi' field.
    """
    json_data = {
        "results": [
            {
                "raw_text": "Sample Text",
                "confidence": 0.98,
                "data": {
                    "date": "2025-12-21",
                    "place": "09",
                    "race_number": 11,
                    "bet_type": "QUINELLA_PLACE",
                    "method": "FORMATION",
                    "selections": [["03"], ["02", "08"]],
                    "amount": 2500
                }
            }
        ]
    }
    
    response = AnalysisResponse(**json_data)
    assert len(response.results) == 1
    # Since we set default to None, it should be None. 
    # If we set default to False, it would be False.
    # In my edit I set: multi: Optional[bool] = None
    assert response.results[0].data.multi is None

def test_analysis_response_with_true_multi():
    """
    Test that AnalysisResponse can handle 'multi': true.
    """
    json_data = {
        "results": [
            {
                "raw_text": "Sample Text",
                "confidence": 0.98,
                "data": {
                    "multi": True
                }
            }
        ]
    }
    
    response = AnalysisResponse(**json_data)
    assert response.results[0].data.multi is True
