from app.schemas import AnalysisResponse

def test_analysis_response_with_null_multi():
    """
    Test that AnalysisResponse can handle 'multi': null in the JSON data.
    This reproduces the error reported by the user.
    """
    json_data = {
        "results": [
            {
                "race": {
                    "date": "2025-12-21",
                    "place": "09",
                    "race_number": 11
                },
                "tickets": [
                    {
                        "receipt_unique_id": None,
                        "bet_type": "QUINELLA_PLACE",
                        "buy_type": "FORMATION",
                        "content": {
                            "type": "QUINELLA_PLACE",
                            "method": "FORMATION",
                            "multi": None,
                            "selections": [["03"], ["02", "08"]],
                            "axis": None,
                            "partners": None,
                            "positions": None
                        },
                        "amount_per_point": None,
                        "total_points": None,
                        "total_cost": 2500,
                        "confidence": 0.98,
                        "warnings": []
                    }
                ],
                "confidence": 0.98
            }
        ]
    }
    
    response = AnalysisResponse(**json_data)
    assert len(response.results) == 1
    assert response.results[0].tickets[0].content.multi is None
    assert response.results[0].race.race_number == 11

def test_analysis_response_with_missing_multi():
    """
    Test that AnalysisResponse can handle missing 'multi' field.
    """
    json_data = {
        "results": [
            {
                "race": {
                    "date": "2025-12-21",
                    "place": "09",
                    "race_number": 11
                },
                "tickets": [
                    {
                        "receipt_unique_id": None,
                        "bet_type": "QUINELLA_PLACE",
                        "buy_type": "FORMATION",
                        "content": {
                            "type": "QUINELLA_PLACE",
                            "method": "FORMATION",
                            "selections": [["03"], ["02", "08"]]
                        },
                        "amount_per_point": None,
                        "total_points": None,
                        "total_cost": 2500,
                        "confidence": 0.98,
                        "warnings": []
                    }
                ],
                "confidence": 0.98
            }
        ]
    }
    
    response = AnalysisResponse(**json_data)
    assert len(response.results) == 1
    assert response.results[0].tickets[0].content.multi is None

def test_analysis_response_with_true_multi():
    """
    Test that AnalysisResponse can handle 'multi': true.
    """
    json_data = {
        "results": [
            {
                "race": {
                    "date": None,
                    "place": None,
                    "race_number": None
                },
                "tickets": [
                    {
                        "receipt_unique_id": None,
                        "bet_type": "TRIFECTA",
                        "buy_type": "NAGASHI",
                        "content": {
                            "type": "TRIFECTA",
                            "method": "NAGASHI",
                            "multi": True
                        },
                        "amount_per_point": None,
                        "total_points": None,
                        "total_cost": None,
                        "confidence": 0.98,
                        "warnings": []
                    }
                ],
                "confidence": 0.98
            }
        ]
    }
    
    response = AnalysisResponse(**json_data)
    assert response.results[0].tickets[0].content.multi is True
