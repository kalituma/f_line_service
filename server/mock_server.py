from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from server.response_obj import WildfireResponse, FireLocation, VideoInfo, WildfireRequest
from server.routers import sender, video, analysis
app = FastAPI(title="Mock Fline API Server")

# ==================== API Endpoints ====================
app.include_router(sender.router, prefix="/api/wildfire", tags=["Sender"])
app.include_router(video.router, prefix="/api/video", tags=["Video"])
app.include_router(analysis.router, tags=["Analysis"])

@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"status": "ok", "service": "Mock Fline API Server"}


if __name__ == '__main__':
    uvicorn.run(app, host="127.0.0.1", port=8086)
