from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncio

from sv.utils.logger import setup_logger, setup_common_logger
from sv.backend.service.app_state_manager import get_app_state_manager, AppState
from sv.daemon.module.recovery_check import RecoveryCheckService
from sv.backend.service.blocking_middleware import InitializationBlockerMiddleware
from sv.backend.service.service_manager import get_service_manager
from sv.routers.fline_web_server import WebThreadManager

logger = setup_logger(__name__)

recovery_service = RecoveryCheckService()
service_manager = get_service_manager()
app_state = get_app_state_manager()

def initialize_logger() -> None:
    setup_common_logger(None)

def setup_recovery_tasks():
    """RecoveryCheckService에 초기화 작업 등록"""

    # Task 1: Processing to Pending 준비
    async def change_works_to_pending():
        """
        시스템 복구 시 PROCESSING 상태의 모든 작업을 PENDING으로 변경
        (비정상 종료 후 재시작 시 진행 중이던 작업 복구)
        """
        logger.info("=" * 80)
        logger.info("🔄 Recovering works from PROCESSING to PENDING...")
        logger.info("=" * 80)
        
        try:
            from sv.backend.work_status import WorkStatus
            
            # WorkQueueService 가져오기
            work_queue_service = service_manager.get_work_queue_service()
            if not work_queue_service:
                logger.error("❌ WorkQueueService not available")
                return False
            
            # PROCESSING 상태의 모든 작업 조회
            logger.info("📋 Fetching works with PROCESSING status...")
            processing_works = work_queue_service.get_works_by_status(WorkStatus.PROCESSING)
            
            if not processing_works:
                logger.info("✓ No works in PROCESSING status")
                return True
            
            logger.info(f"📊 Found {len(processing_works)} works in PROCESSING status")
            
            # PROCESSING 상태의 작업을 PENDING으로 변경
            success_count = 0
            failed_count = 0
            
            for work in processing_works:
                work_id = work.get('work_id')
                logger.info(f"  → Updating work_id={work_id} to PENDING...")
                
                try:
                    result = work_queue_service.update_work_status(work_id, WorkStatus.PENDING)
                    if result:
                        success_count += 1
                        logger.info(f"    ✓ work_id={work_id} changed to PENDING")
                    else:
                        failed_count += 1
                        logger.warning(f"    ✗ Failed to change work_id={work_id} status")
                except Exception as e:
                    failed_count += 1
                    logger.error(f"    ✗ Error updating work_id={work_id}: {str(e)}")
            
            logger.info("=" * 80)
            logger.info(f"✓ Recovery complete: {success_count} succeeded, {failed_count} failed")
            logger.info("=" * 80)
            
            return failed_count == 0
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error(f"❌ Error during work recovery: {str(e)}")
            logger.error("=" * 80)
            return False

    # 초기화 작업 등록
    recovery_service.add_task("change_works_to_pending", change_works_to_pending)

async def restore_and_init() -> bool:
    """
    복구 및 초기화 수행 (RecoveryCheckService 실행)

    Returns:
        bool: 성공 여부
    """
    logger.info("=" * 80)
    logger.info("Starting Daemon Initialization...")
    logger.info("=" * 80)

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

    # RecoveryCheckService의 모든 작업 실행
    success = await recovery_service.run_all()

    if not success:
        await app_state.set_state(AppState.SHUTDOWN)
        logger.error("=" * 80)
        logger.error("❌ Daemon Initialization FAILED!")
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
    from sv.routers import work_queue

    app = FastAPI(
        title="F-line Server to accept job requests from backend",
        lifespan=lifespan_for_test
    )
    
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

def main():
    initialize_logger()
    setup_recovery_tasks()

    web_app = init_web_server()
    web_thread_manager = WebThreadManager(web_app)
    web_thread_manager.start()

    logger.info("=" * 80)
    logger.info("서버 시작 중... 초기화를 기다리는 중...")
    logger.info("=" * 80)

    # 서버가 준비될 때까지 대기 (최대 30초)
    app_state = get_app_state_manager()
    if app_state.wait_until_ready(timeout=30):
        logger.info("=" * 80)
        logger.info("✅ 서버가 완전히 준비되었습니다!")
        logger.info("=" * 80)
        
        # 이제 다른 스레드들을 안전하게 시작할 수 있습니다
        # 예: daemon_thread.start()
        # 예: monitoring_thread.start()
        
        logger.info("다른 작업 스레드를 시작할 수 있습니다...")
    else:
        logger.error("=" * 80)
        logger.error("❌ 서버 초기화 타임아웃! (30초 초과)")
        logger.error("=" * 80)
        web_thread_manager.stop()
        return

    # 메인 스레드에서 다른 작업 수행
    import time
    try:
        while True:
            time.sleep(1)
            # 여기서 다른 작업 가능
    except KeyboardInterrupt:
        logger.info("종료 요청을 받았습니다...")
        web_thread_manager.stop()

if __name__ == "__main__":
    main()