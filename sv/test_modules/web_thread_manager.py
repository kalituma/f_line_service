from typing import Optional, Any
from threading import Thread, Event as ThreadEvent
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncio

from sv.utils.logger import setup_logger, setup_common_logger
from sv.backend.service.app_state_manager import get_app_state_manager, AppState
from sv.daemon.module.recovery_check import RecoveryCheckService
from sv.backend.service.blocking_middleware import InitializationBlockerMiddleware
from sv.backend.service.service_manager import get_service_manager

logger = setup_logger(__name__)

def initialize_logger() -> None:
    setup_common_logger(None)

recovery_service = RecoveryCheckService()
app_state = get_app_state_manager()
service_manager = get_service_manager()

async def restore_and_init() -> bool:
    """
    복구 및 초기화 수행 (RecoveryCheckService 실행)

    Returns:
        bool: 성공 여부
    """
    logger.info("=" * 80)
    logger.info("Starting Daemon Initialization...")
    logger.info("=" * 80)

    # RecoveryCheckService의 모든 작업 실행
    success = await recovery_service.run_all()

    if not success:
        await app_state.set_state(AppState.SHUTDOWN)
        logger.error("=" * 80)
        logger.error("❌ Daemon Initialization FAILED!")
        logger.error("=" * 80)
        return False

    logger.info("=" * 80)
    logger.info("Initializing all services...")
    logger.info("=" * 80)
    
    service_init_success = service_manager.initialize_all_services()
    
    if not service_init_success:
        await app_state.set_state(AppState.SHUTDOWN)
        logger.error("=" * 80)
        logger.error("❌ Service Initialization FAILED!")
        logger.error("=" * 80)
        return False

    await app_state.set_state(AppState.READY)
    logger.info("=" * 80)
    logger.info("✅ Daemon is READY!")
    logger.info("=" * 80)

    return True

@asynccontextmanager
async def lifespan_for_test(app: FastAPI):
    """
    애플리케이션 생명주기 관리

    yield 이전: 시작 시 실행 (startup)
    yield 이후: 종료 시 실행 (shutdown)
    """
    # ==================== Startup ====================
    app_state = get_app_state_manager()

    logger.info("=" * 60)
    logger.info("Server Startup: Initialization in progress...")
    logger.info("=" * 60)

    # 초기화 작업 실행
    success = await restore_and_init()

    if success:
        await app_state.set_state(AppState.READY)
        logger.info("=" * 60)
        logger.info("Server is READY to accept requests!")
        logger.info("=" * 60)
    else:
        logger.error("=" * 60)
        logger.error("Initialization FAILED!")
        logger.error("=" * 60)
        await app_state.set_state(AppState.SHUTDOWN)
        raise RuntimeError("Server initialization failed")

    yield  # 서버 실행 중

    # ==================== Shutdown ====================
    logger.info("=" * 60)
    logger.info("Server Shutting down...")
    logger.info("=" * 60)

def init_web_server() -> FastAPI:
    from sv.routers import job_queue

    app = FastAPI(
        title="F-line Server to accept job requests from backend",
        lifespan=lifespan_for_test
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
    return app

class FlineWebServer:
    def __init__(self, app, host: str, port: int):
        self.config = uvicorn.Config(app, host=host, port=port, log_level="info", lifespan="on")
        self.server = uvicorn.Server(config=self.config)
        self._thread: Optional[Thread] = None
    
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
    
def main():
    initialize_logger()
    web_app = init_web_server()
    web_thread_manager = WebThreadManager(web_app)
    web_thread_manager.start()

    print("서버가 백그라운드에서 실행 중...")

    # 메인 스레드에서 다른 작업 수행
    import time
    try:
        while True:
            time.sleep(1)
            # 여기서 다른 작업 가능
    except KeyboardInterrupt:
        web_thread_manager.stop()


if __name__ == "__main__":
    main()