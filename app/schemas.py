from pydantic import BaseModel

class IpatLoginRequest(BaseModel):
    inet_id: str
    subscriber_no: str
    pin: str
    p_ars: str
    sync_mode: str = "recent"  # "recent" (IPAT) or "past" (Club JRA-Net Query)