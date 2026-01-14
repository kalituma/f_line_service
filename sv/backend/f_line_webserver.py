from typing import TYPE_CHECKING
from fastapi import FastAPI
from sv.utils.logger import setup_logger
from sv.routers import job_queue
from sv.backend.service.app_state_manager import get_app_state_manager
from sv.backend.service.blocking_middleware import InitializationBlockerMiddleware
from sv.backup.initialize_and_recovery import lifespan

if TYPE_CHECKING:
    from sv.daemon.fline_daemon import FlineDaemon

logger = setup_logger(__name__)

def initialize_web_app(daemon: "FlineDaemon") -> FastAPI:
    """
    FastAPI 앱 초기화 및 설정
    
    """
    app = FastAPI(
        title="F-line Server to accept job requests from backend",
        lifespan=lifespan
    )
    
    app.add_middleware(InitializationBlockerMiddleware)
    
    # ==================== API Endpoints ====================
    app.include_router(job_queue.router)
    
    @app.get("/health")
    async def health_check():
        """헬스 체크 엔드포인트 (초기화 상태 확인용)"""
        app_state = get_app_state_manager()
        return {
            "status": "ok",
            "service": "F-line Server",
            "app_state": app_state.get_state().value
        }
    
    app.state.daemon = daemon
    
    return app