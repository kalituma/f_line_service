from typing import Dict, Any, Optional
from datetime import datetime

try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

from sv import LOG_DIR_PATH
from sv.utils.logger import setup_common_logger, setup_logger

from sv.backend.service.app_state import get_app_state_manager, AppState
from sv.daemon.module.recovery_check import RecoveryCheckService

from sv.daemon.module.job_manager import JobManager
from sv.daemon.module.task_manager import TaskManager
from sv.daemon.module.event_processor import EventProcessor
from sv.daemon.module.execution_engine import ExecutionEngine
from sv.daemon.module.thread_manager import ThreadManager

from sv.backend.f_line_server import initialize_web_app

logger = setup_logger(__name__)

def initialize_logger(log_dir_path=None):
    """
    공통 로거 초기화 (실행 시간을 파일명에 포함)

    Args:
        log_dir_path: 로그 디렉토리 경로 (기본값: sv.LOG_DIR_PATH)

    Returns:
        Path: 생성된 로그 파일 경로
    """
    if log_dir_path is None:
        log_dir_path = LOG_DIR_PATH

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir_path / f"f_line_server_{timestamp}.log"
    setup_common_logger(log_file)

    return log_file

class FlineDaemon:
    """
    이벤트 기반 Daemon
    """
    def __init__(
        self,
        num_executors: int = 2,
        poll_interval: float = 2.0,
        fallback_poll_interval: int = 30,
        enable_fallback_polling: bool = True,
        enable_event_listener: bool = True,
        web_host: str = "localhost",
        web_port: int = 8090,
    ):
        """
        Args:
            num_executors: Ray Actor 개수
            poll_interval: Event Listener 폴링 간격
            fallback_poll_interval: Fallback Polling 간격
            enable_fallback_polling: Fallback Polling 활성화
            enable_event_listener: Event Listener 활성화
        """

        self.web_app = initialize_web_app(self)
        self.recovery_service = RecoveryCheckService()
        self.app_state = get_app_state_manager()

        # ==================== 컴포넌트 초기화 ====================
        self.job_manager = JobManager()
        self.task_manager = TaskManager()
        self.execution_engine = ExecutionEngine(num_executors)
        self.event_processor = EventProcessor(
            poll_interval=poll_interval,
            on_job_created=self._on_job_created
        )
        self.thread_manager = ThreadManager(
            poll_interval=poll_interval,
            fallback_poll_interval=fallback_poll_interval,
            enable_event_listener=enable_event_listener,
            enable_fallback_polling=enable_fallback_polling,
            web_app=self.web_app,
            web_host=web_host,
            web_port=web_port
        )
        
        # Thread Manager 콜백 설정
        self.thread_manager.set_check_changes_callback(
            self.event_processor.check_changes
        )
        self.thread_manager.set_process_jobs_callback(
            self._process_pending_jobs
        )
        
        logger.info("=" * 80)
        logger.info("   FlineDaemon initialized")
        logger.info(f"  Executors: {num_executors}")
        logger.info(f"  Poll Interval: {poll_interval}s")
        logger.info(f"  Event Listener: {enable_event_listener}")
        logger.info(f"  Fallback Polling: {enable_fallback_polling}")
        logger.info("=" * 80)
    
    # ==================== Job Management API ====================
    
    def add_job(self, frfr_id: str, analysis_id: str) -> Optional[int]:
        """
        새로운 Job 추가
        
        Args:
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            
        Returns:
            생성된 job_id
        """
        return self.job_manager.add_job(frfr_id, analysis_id)
    
    def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Job 상태 조회"""
        return self.job_manager.get_job_status(job_id)
    
    # ==================== Task Management API ====================
    
    def register_primary_task(self, task):
        """Primary Task 등록"""
        self.task_manager.register_primary_task(task)
    
    def register_secondary_tasks(self, tasks):
        """Secondary Tasks 등록"""
        self.task_manager.register_secondary_tasks(tasks)
    
    def set_data_splitter(self, splitter):
        """데이터 분할 함수 등록"""
        self.task_manager.set_data_splitter(splitter)
    
    # ==================== Internal Event Handlers ====================
    
    async def _on_job_created(self) -> None:
        """Job 생성 감지 시 핸들러 (Event Processor에서 호출)"""
        logger.info("Job creation event detected")
        await self._process_pending_jobs()
    
    async def _process_pending_jobs(self) -> None:
        """
        대기 중인 Job 처리
        
        Flow:
        1. 다음 PENDING Job 가져오기
        2. Task 실행
        3. 결과 로깅
        4. 상태 업데이트
        """
        job_id = self.job_manager.get_next_pending_job()
        
        if not job_id:
            logger.debug("No pending jobs")
            return
        
        # Task 실행 준비 확인
        if not self.task_manager.are_tasks_ready():
            logger.error("❌ Tasks not ready for execution")
            self.job_manager.update_job_status(job_id, "failed")
            return
        
        # Task 실행
        result = await self.execution_engine.execute_job(
            job_id=job_id,
            primary_task=self.task_manager.primary_task,
            secondary_tasks=self.task_manager.secondary_tasks,
            data_splitter=self.task_manager.data_splitter
        )
        
        # 결과 로깅
        self.execution_engine.log_execution_result(job_id, result)
        
        # 상태 업데이트
        status = "completed" if result.get('status') == 'success' else "failed"
        self.job_manager.update_job_status(job_id, status)

    async def restore_and_init(self) -> bool:
        """
        복구 및 초기화 수행 (RecoveryCheckService 실행)

        Returns:
            bool: 성공 여부
        """
        logger.info("=" * 80)
        logger.info("Starting Daemon Initialization...")
        logger.info("=" * 80)

        # RecoveryCheckService의 모든 작업 실행
        success = await self.recovery_service.run_all()

        if success:
            await self.app_state.set_state(AppState.READY)
            logger.info("=" * 80)
            logger.info("✅ Daemon is READY!")
            logger.info("=" * 80)
        else:
            await self.app_state.set_state(AppState.SHUTDOWN)
            logger.error("=" * 80)
            logger.error("❌ Daemon Initialization FAILED!")
            logger.error("=" * 80)

        return success

    # ==================== Lifecycle Management ====================
    
    def start(self) -> None:
        """Daemon 시작"""
        logger.info("Starting FlineDaemon...")
        self.thread_manager.start()
        logger.info("FlineDaemon started successfully")
    
    def stop(self) -> None:
        """Daemon 중지"""
        logger.info("Stopping FlineDaemon...")
        self.thread_manager.stop()
        logger.info("FlineDaemon stopped")
    
    # ==================== Status API ====================
    
    def get_status(self) -> Dict[str, Any]:
        """Daemon 상태 조회"""
        return {
            'daemon': {
                'running': self.thread_manager.running
            },
            'tasks': self.task_manager.get_tasks_summary(),
            'threads': self.thread_manager.get_status(),
            'executor': self.execution_engine.get_executor_status()
        }
    
    def get_summary(self) -> str:
        """Daemon 상태 요약 (로깅용)"""
        status = self.get_status()
        return (
            f"Running: {status['daemon']['running']} | "
            f"Tasks: {status['tasks']['secondary_tasks_count']} | "
            f"Listener: {status['threads']['listener_active']} | "
            f"Polling: {status['threads']['polling_active']}"
        )

if __name__ == '__main__':
    # 1. Ray 초기화
    ray.init(num_cpus=8, ignore_reinit_error=True)

    log_file = initialize_logger()
    app_state = get_app_state_manager()

    logger.info("Starting F-line Server with Daemon...")
    logger.info(f"Log file: {log_file}")
    logger.info(f"App state: {app_state.get_state().value}")

    daemon = FlineDaemon()
    # daemon.register_primary_task(DataCollectionTask())
    # daemon.register_secondary_tasks([
    #     ProcessingTask(),
    #     SavingTask()
    # ])
    # daemon.set_data_splitter(split_items)