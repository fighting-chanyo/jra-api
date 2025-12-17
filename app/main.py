from fastapi import FastAPI
from app.routers import ipat, races

app = FastAPI()

app.include_router(ipat.router, prefix="/api")
app.include_router(races.router, prefix="/api")

@app.get("/")
def health_check():
    return {"status": "ok", "message": "JRA IPAT Scraper API is running."}
