from pydantic import BaseModel, Field

class IpatAuth(BaseModel):
    inet_id: str
    subscriber_number: str
    password: str
    pars_number: str

class SyncIpatRequest(BaseModel):
    log_id: str
    user_id: str
    ipat_auth: IpatAuth
    mode: str = "past"

from typing import Optional, List, Dict, Any
from datetime import date, datetime

class PayoutItem(BaseModel):
    horse: List[int]
    money: int

class PayoutData(BaseModel):
    WIN: Optional[List[PayoutItem]] = None
    PLACE: Optional[List[PayoutItem]] = None
    BRACKET_QUINELLA: Optional[List[PayoutItem]] = None
    QUINELLA: Optional[List[PayoutItem]] = None
    QUINELLA_PLACE: Optional[List[PayoutItem]] = None
    EXACTA: Optional[List[PayoutItem]] = None
    TRIO: Optional[List[PayoutItem]] = None
    TRIFECTA: Optional[List[PayoutItem]] = None

class Race(BaseModel):
    id: str
    date: date
    place_code: str
    race_number: int
    name: Optional[str] = None
    result_1st: Optional[str] = None
    result_2nd: Optional[str] = None
    result_3rd: Optional[str] = None
    payout_data: Optional[PayoutData] = None
    status: str = "BEFORE"
    post_time: Optional[datetime] = None
    external_id: Optional[str] = None

class Ticket(BaseModel):
    id: Optional[str] = None
    user_id: str
    race_id: str
    bet_type: str
    buy_type: str
    content: Dict[str, Any]
    amount_per_point: int
    total_points: int
    total_cost: int
    status: str = "PENDING"
    payout: Optional[int] = None
    source: str = "IPAT_SYNC"
    created_at: Optional[datetime] = None
    mode: str = "REAL"
    receipt_unique_id: Optional[str] = None
