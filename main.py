from fastapi import FastAPI
from app.schemas import IpatLoginRequest
from app.services.ipat_scraper import sync_recent_history, sync_past_history

app = FastAPI()

@app.post("/api/sync")
def sync_ipat_data(creds: IpatLoginRequest):
    print(f"🔄 Sync request received. Mode: {creds.sync_mode}")
    
    if creds.sync_mode == "past":
        return sync_past_history(creds)
    else:
        return sync_recent_history(creds)