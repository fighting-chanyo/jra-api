import pytest
from app.services.judgment_logic import JudgmentLogic
from app.schemas import Ticket, BetContent, PayoutData, PayoutItem
from datetime import date

def test_judge_ticket_with_pydantic_content():
    # Create a Ticket with BetContent object
    content = BetContent(
        type="WIN",
        method="NORMAL",
        multi=False,
        selections=[["1"]],
        axis=[],
        partners=[],
        positions=[]
    )
    
    ticket = Ticket(
        user_id="test_user",
        race_id="test_race",
        bet_type="WIN",
        buy_type="NORMAL",
        content=content,
        amount_per_point=100,
        total_points=1,
        total_cost=100
    )
    
    # Create PayoutData
    payout_data = PayoutData(
        WIN=[PayoutItem(horse=[1], money=200)]
    )
    
    # Call judge_ticket
    # r1, r2, r3 are just passed but might not be used for WIN if payout_data is sufficient
    # But let's pass consistent data
    status, payout = JudgmentLogic.judge_ticket(ticket, 1, 2, 3, payout_data)
    
    assert status == "HIT"
    assert payout == 200

def test_judge_ticket_lose():
    # Create a Ticket with BetContent object
    content = BetContent(
        type="WIN",
        method="NORMAL",
        multi=False,
        selections=[["2"]],
        axis=[],
        partners=[],
        positions=[]
    )
    
    ticket = Ticket(
        user_id="test_user",
        race_id="test_race",
        bet_type="WIN",
        buy_type="NORMAL",
        content=content,
        amount_per_point=100,
        total_points=1,
        total_cost=100
    )
    
    # Create PayoutData
    payout_data = PayoutData(
        WIN=[PayoutItem(horse=[1], money=200)]
    )
    
    # Call judge_ticket
    status, payout = JudgmentLogic.judge_ticket(ticket, 1, 2, 3, payout_data)
    
    assert status == "LOSE"
    assert payout == 0
