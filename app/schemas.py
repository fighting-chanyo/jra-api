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
    WIN: Optional[List[PayoutItem]] = Field(None, alias="TAN")
    PLACE: Optional[List[PayoutItem]] = Field(None, alias="FUKU")
    BRACKET_QUINELLA: Optional[List[PayoutItem]] = Field(None, alias="WAKUREN")
    QUINELLA: Optional[List[PayoutItem]] = Field(None, alias="UMAREN")
    QUINELLA_PLACE: Optional[List[PayoutItem]] = Field(None, alias="WIDE")
    EXACTA: Optional[List[PayoutItem]] = Field(None, alias="UMATAN")
    TRIO: Optional[List[PayoutItem]] = Field(None, alias="SANRENPUKU")
    TRIFECTA: Optional[List[PayoutItem]] = None

    class Config:
        populate_by_name = True

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

class BetContent(BaseModel):
    type: str
    method: str
    multi: bool
    selections: List[List[str]]
    axis: List[str]
    partners: List[str]
    positions: List[int]

class Ticket(BaseModel):
    id: Optional[str] = None
    user_id: str
    race_id: str
    bet_type: str
    buy_type: str
    content: BetContent
    amount_per_point: int
    total_points: int
    total_cost: int
    status: str = "PENDING"
    payout: Optional[int] = None
    source: str = "IPAT_RECENT"
    created_at: Optional[datetime] = None
    mode: str = "REAL"
    receipt_unique_id: Optional[str] = None

# --- Image Analysis Models ---

from enum import Enum

class BetType(str, Enum):
    WIN = "WIN"
    PLACE = "PLACE"
    BRACKET_QUINELLA = "BRACKET_QUINELLA"
    QUINELLA = "QUINELLA"
    QUINELLA_PLACE = "QUINELLA_PLACE"
    EXACTA = "EXACTA"
    TRIO = "TRIO"
    TRIFECTA = "TRIFECTA"

class BuyType(str, Enum):
    NORMAL = "NORMAL"
    BOX = "BOX"
    FORMATION = "FORMATION"
    NAGASHI = "NAGASHI"

class AnalyzeQueueRequest(BaseModel):
    queueId: str

class RaceInfo(BaseModel):
    date: Optional[str] = None
    place: Optional[str] = None
    race_number: Optional[int] = None

class TicketContent(BaseModel):
    type: str
    method: str
    multi: bool
    selections: List[List[str]]
    axis: List[str]
    partners: List[str]
    positions: List[int]

class TicketCandidate(BaseModel):
    receipt_unique_id: Optional[str] = None
    bet_type: str
    buy_type: str
    content: TicketContent
    amount_per_point: Optional[int] = None
    total_points: Optional[int] = None
    total_cost: Optional[int] = None
    confidence: float
    warnings: List[str] = []

class AnalysisResult(BaseModel):
    race: RaceInfo
    tickets: List[TicketCandidate]
    confidence: float

class AnalysisResponse(BaseModel):
    results: List[AnalysisResult]

