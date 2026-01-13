from typing import TYPE_CHECKING
from fastapi import FastAPI
from contextlib import asynccontextmanager

from sv.utils.logger import setup_logger
from sv.backend.service.app_state_manager import get_app_state_manager, AppState

if TYPE_CHECKING:
    from sv.daemon.fline_daemon import FlineDaemon

logger = setup_logger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    애플리케이션 생명주기 관리

    yield 이전: 시작 시 실행 (startup)
    yield 이후: 종료 시 실행 (shutdown)
    """
    # ==================== Startup ====================
    daemon:"FlineDaemon" = app.state.daemon
    app_state = get_app_state_manager()

    logger.info("=" * 60)
    logger.info("Server Startup: Initialization in progress...")
    logger.info("=" * 60)

    # 초기화 작업 실행
    success = await daemon.restore_and_init()

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