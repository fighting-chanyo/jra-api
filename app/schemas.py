from pydantic import BaseModel, Field

class IpatAuth(BaseModel):
    inet_id: str
    subscriber_number: str = Field(..., alias="subscriber_no") # alias for request body compatibility
    password: str = Field(..., alias="pin")
    pars_number: str = Field(..., alias="p_ars")

class SyncIpatRequest(BaseModel):
    log_id: str
    user_id: str
    ipat_auth: IpatAuth
    sync_mode: str = "past"