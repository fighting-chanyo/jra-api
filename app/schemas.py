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