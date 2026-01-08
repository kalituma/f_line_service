try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

import logging as logging_module
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
import asyncio
import time
from threading import Thread, Event as ThreadEvent
from fastapi import FastAPI  # type: ignore


from sv import LOG_DIR_PATH
from sv.backend.service.job_queue_service import JobQueueService
from sv.daemon.module.db_change_listener import DBChangeListener, DBChangeEvent, ChangeEventType
from sv.daemon.module.split_executor import FlineTaskSplitExecutor
from sv.task.task_base import TaskBase
from sv.backend.db.job_queue_db import JobStatus
from sv.utils.logger import setup_common_logger, setup_logger

logger = setup_logger(__name__)
logging = logging_module

class FlineDaemon:
    """이벤트 기반 하이브리드 Daemon"""
    
    def __init__(
        self,
        num_executors: int = 2,
        poll_interval: float = 2.0,
        fallback_poll_interval: int = 30,
        enable_fallback_polling: bool = True,
        enable_event_listener: bool = True,
        primary_task: TaskBase = None,
        secondary_tasks: List[TaskBase] = None,
        data_splitter: Optional[Callable[[Dict[str, Any]], List[Any]]] = None,
        db_change_callback: Optional[Callable[[DBChangeEvent], None]] = None
    ):
        """
        Args:
            config: Daemon 설정
            primary_task: 첫 번째 실행 작업 (Task 1)
            secondary_tasks: 데이터 분할 후 각 아이템에서 실행할 작업 리스트
            data_splitter: Task 1 결과를 분할하는 함수
            db_change_callback: DB 변경 감지 시 실행할 추가 콜백
        """
        self.primary_task = primary_task
        self.secondary_tasks = secondary_tasks or []
        self.data_splitter = data_splitter
        self.db_change_callback = db_change_callback
        self.num_executors = num_executors        
        self.poll_interval = poll_interval
        self.fallback_poll_interval = fallback_poll_interval
        self.enable_fallback_polling = enable_fallback_polling
        self.enable_event_listener = enable_event_listener

        self.job_queue_service = JobQueueService()
        
        # DB 변경 리스너 초기화
        self.db_listener = DBChangeListener(poll_interval)
        self.db_listener.on_change(self._handle_db_change)
        
        # Ray Actor 풀
        self.executor_actors = [
            FlineTaskSplitExecutor.remote(i) for i in range(self.num_executors)
        ]
        self.current_executor_idx = 0
        
        # 제어 플래그
        self.running = False
        self.listener_thread: Optional[Thread] = None
        self.fallback_poll_thread: Optional[Thread] = None
        self.stop_event = ThreadEvent()
        
        logger.info("=" * 80)
        logger.info("🚀 EventDrivenDaemon initialized")
        logger.info(f"  Executors: {self.num_executors}")
        logger.info(f"  Poll Interval: {self.poll_interval}s")
        logger.info(f"  Event Listener: {self.enable_event_listener}")
        logger.info(f"  Fallback Polling: {self.enable_fallback_polling}")
        logger.info("=" * 80)
    
    def add_job(self, frfr_id: str, analysis_id: str) -> Optional[int]:
        """
        새로운 Job 추가 (Main Loop에서 호출)
        
        Args:
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            
        Returns:
            생성된 job_id 또는 None (중복 또는 에러)
        """
        try:
            job_id = self.job_queue_service.add_job(frfr_id, analysis_id)
            if job_id:
                logger.info(f"Job added: job_id={job_id}, frfr_id={frfr_id}")
            else:
                logger.warning(f"Job already exists: frfr_id={frfr_id}")
            return job_id
        except Exception as e:
            logger.error(f"Error adding job: {str(e)}", exc_info=True)
            return None


    def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Job 상태 조회
        
        Args:
            job_id: 조회할 job ID
            
        Returns:
            Job 정보 또는 None
        """
        try:
            return self.job_queue_service.get_job_status(job_id)
        except Exception as e:
            logger.error(f"Error getting job status: {str(e)}")
            return None
            
    def register_primary_task(self, task: TaskBase):
        """주요 작업 등록 (Task 1)"""
        self.primary_task = task
        logger.info(f"Primary task registered: {task.task_name}")
    
    def register_secondary_tasks(self, tasks: List[TaskBase]):
        """보조 작업 등록 (Task 2~N)"""
        self.secondary_tasks.extend(tasks)
        logger.info(f"{len(tasks)} secondary tasks registered")
    
    def set_data_splitter(self, splitter: Callable[[Dict[str, Any]], List[Any]]):
        """데이터 분할 함수 등록"""
        self.data_splitter = splitter
        logger.info(f"Data splitter registered: {splitter.__name__}")
    
    def _handle_db_change(self, event: DBChangeEvent):
        """DB 변경 이벤트 핸들러"""
        logger.info(f"🔔 DB Change detected: {event}")
        
        # 추가 콜백 실행
        if self.db_change_callback:
            try:
                self.db_change_callback(event)
            except Exception as e:
                logger.error(f"Error in custom callback: {str(e)}", exc_info=True)
        
        # Job Queue 테이블 변경 시
        if event.table_name == "job_queue" and event.event_type == ChangeEventType.INSERT:
            logger.info(f"New job detected: {event.data}")
            # 즉시 처리
            asyncio.create_task(self._process_pending_jobs())
    
    def _get_next_executor(self) -> ray.actor.ActorHandle:
        """다음 실행자 Actor 반환 (라운드 로빈)"""
        executor = self.executor_actors[self.current_executor_idx]
        self.current_executor_idx = (self.current_executor_idx + 1) % len(self.executor_actors)
        return executor
    
    async def _process_pending_jobs(self):
        """
        대기 중인 Job 처리
        
        Flow:
        1. Job Queue에서 다음 PENDING job 가져오기
        2. Primary Task 실행
        3. 결과 분할
        4. 각 데이터에 대해 Secondary Tasks 실행
        5. 결과 저장
        """
        try:
            # 처리할 Job이 있는지 확인
            job_id = self.job_queue_service.get_next_job()
            
            if not job_id:
                logger.debug("No pending jobs")
                return
            
            logger.info("=" * 80)
            logger.info(f"🔄 Processing job: {job_id}")
            logger.info("=" * 80)
            
            # ==================== 작업 실행 ====================
            
            if not self.primary_task or not self.secondary_tasks:
                logger.error("Primary task or secondary tasks not registered")
                self.job_queue._conn().__enter__().execute(
                    "UPDATE job_queue SET status = ? WHERE job_id = ?",
                    (JobStatus.FAILED.value, job_id)
                )
                return
            
            loop_context = {
                'job_id': job_id,
                'start_time': datetime.now().isoformat(),
                'task_count': len(self.secondary_tasks)
            }
            
            executor = self._get_next_executor()
            
            logger.info(f"Submitting job {job_id} to executor with data splitting")
            
            # 데이터 분할 방식으로 실행
            result_ref = executor.execute_with_data_splitting.remote(
                self.primary_task,
                self.secondary_tasks,
                loop_context,
                self.data_splitter or (lambda x: [x]),  # 기본 분할 함수
                continue_on_error=True
            )
            
            # 결과 대기
            result = ray.get(result_ref)
            
            # 결과 처리
            await self._handle_execution_result(job_id, result)
            
            logger.info("=" * 80)
            logger.info(f"✅ Job {job_id} completed")
            logger.info("=" * 80)
        
        except Exception as e:
            logger.error(f"❌ Error processing pending jobs: {str(e)}", exc_info=True)
    
    async def _handle_execution_result(self, job_id: int, result: Dict[str, Any]):
        """실행 결과 처리"""
        logger.info("Execution result:")
        logger.info(f"  Status: {result.get('status')}")
        logger.info(f"  Total duration: {result.get('total_duration'):.2f}s")
        logger.info(f"  Data items processed: {len(result.get('data_items', []))}")
        logger.info(f"  Errors: {result.get('error_count', 0)}")
        
        # Primary task 결과
        if result.get('primary_task'):
            primary = result['primary_task']
            logger.info(f"  Primary task: {primary['status']} ({primary.get('duration', 0):.2f}s)")
        
        # 각 데이터 아이템 처리 결과
        for item_idx, item_result in enumerate(result.get('data_items', []), 1):
            logger.info(f"  Data item {item_idx}: {item_result['status']} ({item_result.get('duration', 0):.2f}s)")
            for task_info in item_result.get('tasks', []):
                status_icon = "✓" if task_info['status'] == 'success' else "✗"
                logger.info(
                    f"    {status_icon} {task_info['task_name']}: "
                    f"{task_info['status']} ({task_info.get('duration', 0):.2f}s)"
                )
        
        # Job 상태 업데이트
        status = JobStatus.COMPLETED if result['status'] == 'success' else JobStatus.FAILED
        
        try:
            with self.job_queue._conn() as conn:
                conn.execute(
                    "UPDATE job_queue SET status = ? WHERE job_id = ?",
                    (status.value, job_id)
                )
            logger.info(f"Job {job_id} status updated to {status.value}")
        except Exception as e:
            logger.error(f"Error updating job status: {str(e)}")
    
    def _listener_thread_func(self):
        """DB 변경 감지 리스너 스레드"""
        logging.info("🎧 DB Change Listener started")  # noqa: F541
        
        while not self.stop_event.is_set():
            try:
                # Job Queue 테이블 감시 (PENDING 상태)
                self.db_listener.check_status_column_change(
                    table_name="job_queue",
                    status_column="status",
                    target_status=JobStatus.PENDING.value,
                    id_column="job_id"
                )
                
                time.sleep(self.poll_interval)
            
            except Exception as e:
                logger.error(f"Error in listener thread: {str(e)}", exc_info=True)  # noqa: F541
                time.sleep(self.poll_interval)
        
        logger.info("🎧 DB Change Listener stopped")
    
    def _fallback_poll_thread_func(self):
        """폴백 주기적 폴링 스레드"""
        logging.info("📊 Fallback Polling started")  # noqa: F541
        
        while not self.stop_event.is_set():
            try:
                logger.debug("Running fallback periodic polling")
                asyncio.run(self._process_pending_jobs())
                
                time.sleep(self.fallback_poll_interval)
            
            except Exception as e:
                logger.error(f"Error in fallback polling: {str(e)}", exc_info=True)  # noqa: F541
                time.sleep(self.fallback_poll_interval)
        
        logger.info("📊 Fallback Polling stopped")
    
    def start(self):
        """Daemon 시작"""
        logger.info("Starting EventDrivenDaemon...")
        
        if self.running:
            logger.warning("Daemon is already running")
            return
        
        self.running = True
        self.stop_event.clear()
        
        # 리스너 스레드 시작
        if self.enable_event_listener:
            self.listener_thread = Thread(
                target=self._listener_thread_func,
                daemon=True,
                name="DBChangeListener"
            )
            self.listener_thread.start()
        
        # 폴백 폴링 스레드 시작
        if self.enable_fallback_polling:
            self.fallback_poll_thread = Thread(
                target=self._fallback_poll_thread_func,
                daemon=True,
                name="FallbackPoller"
            )
            self.fallback_poll_thread.start()
        
        logger.info("✅ EventDrivenDaemon started successfully")
    
    def stop(self):
        """Daemon 중지"""
        logger.info("Stopping EventDrivenDaemon...")
        
        self.running = False
        self.stop_event.set()
        
        # 스레드 종료 대기
        if self.listener_thread:
            self.listener_thread.join(timeout=5)
        
        if self.fallback_poll_thread:
            self.fallback_poll_thread.join(timeout=5)
        
        logger.info("✅ EventDrivenDaemon stopped")
    
    def get_status(self) -> Dict[str, Any]:
        """Daemon 상태 반환"""
        return {
            'running': self.running,
            'executors': len(self.executor_actors),
            'has_primary_task': self.primary_task is not None,
            'secondary_tasks': len(self.secondary_tasks),
            'listener_active': self.config.enable_event_listener and self.listener_thread and self.listener_thread.is_alive(),
            'fallback_polling_active': self.config.enable_fallback_polling and self.fallback_poll_thread and self.fallback_poll_thread.is_alive()
        }

def initialize_logger(log_dir_path=None):
    """공통 로거 초기화"""
    if log_dir_path is None:
        log_dir_path = LOG_DIR_PATH
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir_path / f"f_line_server_{timestamp}.log"
    setup_common_logger(log_file)
    
    return log_file


if __name__ == '__main__':
    # 1. 로거 초기화
    log_file = initialize_logger()
    
    # 2. Ray 초기화
    ray.init(num_cpus=8, ignore_reinit_error=True)

    # 4. Daemon 생성
    daemon = FlineDaemon()
    
    # ✅ TODO: Task 등록 (사용자가 구현해야 함)
    # from sv.task.your_tasks import Task1, Task2, Task3
    # 
    # def split_result(result):
    #     return result.get('items', [])
    # 
    # daemon.register_primary_task(Task1())
    # daemon.register_secondary_tasks([Task2(), Task3()])
    # daemon.set_data_splitter(split_result)
    
    logger.info("Daemon configured and ready to use")
    logger.info(f"Daemon status: {daemon.get_status()}")

