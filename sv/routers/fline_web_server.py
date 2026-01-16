from typing import TYPE_CHECKING
from typing import Optional, Any
from threading import Thread, Event as ThreadEvent
import uvicorn
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncio

from sv.backend.service.app_state_manager import get_app_state_manager, AppState
from sv.backend.service.blocking_middleware import InitializationBlockerMiddleware
from sv.utils.logger import setup_logger, setup_common_logger
from sv.utils.initialize_and_recovery import lifespan

if TYPE_CHECKING:
    from sv.daemon.fline_daemon import FlineDaemon

logger = setup_logger(__name__)

def init_web_server(daemon:"FlineDaemon") -> FastAPI:
    from sv.routers import job_queue
    from sv.routers import work_queue

    app = FastAPI(
        title="F-line Server to accept job requests from backend",
        lifespan=lifespan
    )
    app.state.daemon = daemon
    app.add_middleware(InitializationBlockerMiddleware)

    # ==================== API Endpoints ====================
    app.include_router(job_queue.router)
    app.include_router(work_queue.router)

    @app.get("/health")
    async def health_check():
        """헬스 체크 엔드포인트 (초기화 상태 확인용)"""
        app_state = get_app_state_manager()
        return {
            "status": "ok",
            "service": "F-line Server",
            "app_state": app_state.get_state().value
        }

    return app

class FlineWebServer:
    def __init__(self, app, host: str, port: int):
        self.config = uvicorn.Config(app, host=host, port=port, log_level="info", lifespan="on")
        self.server = uvicorn.Server(config=self.config)
        self._thread: Optional[Thread] = None
        self.ready_event = ThreadEvent()  # 준비 완료 이벤트

    def start(self) -> None:
        if self._thread is not None:
            logger.warning("⚠️ Web Server is already running")
            return
        self._thread = Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("✓ Web Server thread started")

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.server.serve())

    def stop(self) -> None:
        if self._thread is None:
            logger.warning("⚠️ Web Server is not running")
            return
        self._thread.join(timeout=5)
        logger.info("✓ Web Server thread stopped")

    def wait_until_ready(self, timeout: Optional[float] = 30) -> bool:
        """서버가 준비될 때까지 대기"""
        return self.ready_event.wait(timeout=timeout)

class WebThreadManager:
    def __init__(
            self,
            web_app: Optional[Any] = None,
            web_host: str = "localhost",
            web_port: int = 8090
    ):
        self.web_server = FlineWebServer(web_app, web_host, web_port)
        self.running = False
        self.stop_event = ThreadEvent()

    def start(self) -> None:
        if self.running:
            logger.warning("⚠️ ThreadManager is already running")
            return
        self.running = True
        self.stop_event.clear()
        self.web_server.start()
        logger.info("✓ Web Server thread started")

    def stop(self) -> None:
        if not self.running:
            logger.warning("⚠️ Web Server is not running")
            return
        self.running = False
        self.stop_event.set()
        self.web_server.stop()
        logger.info("✓ Web Server thread stopped")