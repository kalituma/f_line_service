import requests  # type: ignore
from apscheduler.schedulers.blocking import BlockingScheduler  # type: ignore
import ray  # type: ignore
from typing import List, Dict, Any
import uvicorn  # type: ignore
from datetime import datetime
import asyncio
from threading import Thread

from sv import LOG_DIR_PATH
from sv.backup.monitor import FlineTaskMonitor
from sv.backup.executor import FlineTaskExecutor, TaskBase
from sv.utils.logger import setup_common_logger, setup_logger
from sv.backend.service.app_state_manager import get_app_state_manager, AppState
from sv.daemon.module.recovery_check import RecoveryCheckService
from sv.backend.f_line_webserver import initialize_web_app
from sv.task.tasks import (
    VideoProcessingTask,
    AnalysisTask,
    ReportGenerationTask,
    NotificationTask
)

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

class FLineDaemon:
    """
    순차적 작업 실행을 지원하는 Ray Daemon
    RecoveryCheckService를 내부적으로 통합
    """
    
    def __init__(
        self,
        interval_seconds: int = 900,
        num_executors: int = 5,
        use_actors: bool = True,
        sequential_tasks: List[TaskBase] = None,
        web_host: str = "localhost",
        web_port: int = 8090,
    ):
        """
        Args:
            interval_seconds: 루프 실행 간격 (초)
            num_executors: 실행자(Executor) 개수
            use_actors: Actor 사용 여부
            sequential_tasks: 순차적으로 실행할 작업 목록
            web_host: 웹 서버 호스트
            web_port: 웹 서버 포트
        """
        self.interval_seconds = interval_seconds
        self.sequential_tasks = sequential_tasks or []
        self.task_monitor = FlineTaskMonitor(
            num_executors=num_executors,
            use_actors=use_actors
        )
        self.scheduler = BlockingScheduler()
        self.num_executors = num_executors
        self.use_actors = use_actors
        
        # Ray Actor 풀 생성
        if use_actors:
            self.executor_actors = [
                FlineTaskExecutor.remote(i) for i in range(num_executors)
            ]
        else:
            self.executor_actors = None
        
        self.current_executor_idx = 0
        
        self.web_app = initialize_web_app(self)
        self.web_host = web_host
        self.web_port = web_port
        
        # ==================== RecoveryCheckService 통합 ====================
        self.recovery_service = RecoveryCheckService()
        self.app_state_manager = get_app_state_manager()
        
        # 초기화 작업 등록
        self._setup_recovery_tasks()
        
        logger.info("FLineDaemon initialized")
        logger.info(f"  Interval: {interval_seconds} seconds")
        logger.info(f"  Sequential tasks: {len(self.sequential_tasks)}")
        logger.info(f"  Executors: {num_executors}")
    
    def _setup_recovery_tasks(self):
        """RecoveryCheckService에 초기화 작업 등록"""
        
        # Task 1: Daemon 준비
        async def prepare_daemon():
            """Daemon 준비 작업"""
            logger.info("🔄 Preparing daemon...")
            await asyncio.sleep(0.1)  # 실제 준비 로직
            if not self.sequential_tasks:
                logger.warning("⚠️  No sequential tasks registered")
                return False
            logger.info("✓ Daemon preparation complete")
            return True
        
        # Task 2: 상태 초기화
        async def initialize_state():
            """애플리케이션 상태 초기화"""
            logger.info("🔄 Initializing application state...")
            await asyncio.sleep(0.1)
            await self.app_state_manager.set_state(AppState.INITIALIZING)
            logger.info("✓ State initialization complete")
            return True
        
        # Task 3: 리소스 확인
        async def check_resources():
            """Ray 리소스 확인"""
            logger.info("🔄 Checking Ray resources...")
            await asyncio.sleep(0.1)
            resources = ray.available_resources()
            logger.info(f"  Available resources: {resources}")
            logger.info("✓ Resource check complete")
            return True
        
        # 초기화 작업 등록
        self.recovery_service.add_task("prepare_daemon", prepare_daemon)
        self.recovery_service.add_task("initialize_state", initialize_state)
        self.recovery_service.add_task("check_resources", check_resources)

    def register_sequential_task(self, task: TaskBase):
        """순차 작업 등록
        
        Args:
            task: 실행할 작업 (TaskBase 인스턴스)
        """
        self.sequential_tasks.append(task)
        logger.info(f"Task registered: {task.task_name}")

    def register_sequential_tasks(self, tasks: List[TaskBase]):
        """여러 순차 작업 등록
        
        Args:
            tasks: 실행할 작업 리스트
        """
        self.sequential_tasks.extend(tasks)
        logger.info(f"{len(tasks)} tasks registered")

    def loop_trigger(self):
        """주기적으로 실행되는 루프 - 순차 작업 실행"""
        
        if not self.sequential_tasks:
            logger.warning("No sequential tasks registered")
            return
        
        logger.info("=" * 80)
        logger.info(f"🔄 Loop triggered at {datetime.now().isoformat()}")
        logger.info("=" * 80)
        
        try:
            # 루프 컨텍스트 생성
            loop_context = {
                'loop_time': datetime.now().isoformat(),
                'interval_seconds': self.interval_seconds,
                'task_count': len(self.sequential_tasks)
            }
            
            # Actor에서 순차 작업 실행
            executor_actor = self.executor_actors[self.current_executor_idx]
            self.current_executor_idx = (self.current_executor_idx + 1) % len(self.executor_actors)
            
            logger.info(f"Submitting {len(self.sequential_tasks)} sequential tasks to Executor")
            
            # 원격 함수로 순차 작업 실행
            result_ref = executor_actor.execute_sequential_tasks.remote(
                self.sequential_tasks,
                loop_context
            )
            
            # 결과 대기 및 수신
            result = ray.get(result_ref)
            
            # 결과 처리
            self._handle_loop_result(result)
            
            logger.info("=" * 80)
            logger.info("✅ Loop completed successfully")
            logger.info("=" * 80)
        
        except Exception as e:
            logger.error(f"❌ Error in loop_trigger: {str(e)}", exc_info=True)
            logger.info("=" * 80)

    def _handle_loop_result(self, result: Dict[str, Any]):
        """루프 실행 결과 처리"""
        logger.info("Loop execution result:")
        logger.info(f"  Status: {result.get('status')}")
        logger.info(f"  Total duration: {result.get('total_duration'):.2f}s")
        logger.info(f"  Tasks executed: {len(result.get('tasks', []))}")
        
        for task_info in result.get('tasks', []):
            task_name = task_info.get('task_name', 'unknown')
            task_status = task_info.get('status', 'unknown')
            task_duration = task_info.get('duration', 0)
            
            status_icon = "✓" if task_status == 'success' else "✗"
            logger.info(f"  {status_icon} {task_name}: {task_status} ({task_duration:.2f}s)")


    async def restore_and_init(self) -> bool:
        """
        복구 및 초기화 수행 (RecoveryCheckService 실행)
        
        Returns:
            bool: 성공 여부
        """
        logger.info("=" * 80)
        logger.info("🚀 Starting Daemon Initialization...")
        logger.info("=" * 80)
        
        # RecoveryCheckService의 모든 작업 실행
        success = await self.recovery_service.run_all()
        
        if success:
            await self.app_state_manager.set_state(AppState.READY)
            logger.info("=" * 80)
            logger.info("✅ Daemon is READY!")
            logger.info("=" * 80)
        else:
            await self.app_state_manager.set_state(AppState.SHUTDOWN)
            logger.error("=" * 80)
            logger.error("❌ Daemon Initialization FAILED!")
            logger.error("=" * 80)
        
        return success

    def start(self):
        """데몬 시작"""
        logger.info("Starting Ray daemon...")
        logger.info(f"Ray cluster info: {ray.cluster_resources()}")
        logger.info(f"Scheduling loop to run every {self.interval_seconds} seconds")

        # ==================== 스케줄러 및 웹 서버 시작 ====================
        # 스케줄러 작업 등록
        self.scheduler.add_job(
            self.loop_trigger,
            'interval',
            seconds=self.interval_seconds,
            id='loop_trigger_job',
            next_run_time=datetime.now()
        )

        # 웹 서버를 별도 스레드에서 실행
        web_thread = Thread(
            target=lambda: uvicorn.run(
                self.web_app,
                host=self.web_host,
                port=self.web_port,
                log_level="info"
            ),
            daemon=True,
            name="WebServer"
        )
        web_thread.start()
        logger.info(f"Web server started: http://{self.web_host}:{self.web_port}")

        # 스케줄러 시작 (메인 스레드)
        try:
            logger.info("🔄 Starting scheduler loop...")
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Received shutdown signal")
            self.shutdown()

    def shutdown(self):
        """종료"""
        logger.info("Shutting down Ray daemon...")
        self.scheduler.shutdown(wait=False)
        self.task_monitor.shutdown()
        ray.shutdown()
        logger.info("Ray daemon stopped")


if __name__ == '__main__':
    # 1. Ray 초기화
    ray.init(num_cpus=8, ignore_reinit_error=True)
    
    # 2. 공통 로거 초기화 (실행 시간을 파일명에 포함)
    log_file = initialize_logger()
    
    logger = setup_logger(__name__)
    app_state = get_app_state_manager()
    
    logger.info("Starting F-line Server with Daemon...")
    logger.info(f"Log file: {log_file}")
    logger.info(f"App state: {app_state.get_state().value}")   
    
    
    # 4. 사용 예시 (Daemon 객체 생성)
    daemon = FLineDaemon(
        interval_seconds=3,
        num_executors=2,
        use_actors=True,
        web_host="localhost",
        web_port=8090,
    )

    tasks = [
        VideoProcessingTask(),
        AnalysisTask(),
        ReportGenerationTask(),
        NotificationTask()
    ]
    
    daemon.register_sequential_tasks(tasks)
    
    logger.info("Daemon and Server initialized")

    try:
        daemon.start()
    except Exception as e:
        logger.error(f"Daemon error: {str(e)}")
        daemon.shutdown()