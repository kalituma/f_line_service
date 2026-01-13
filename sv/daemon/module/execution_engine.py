from typing import Dict, Any, Callable, Optional, Tuple, List
from datetime import datetime
from threading import Lock
import os

try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

from sv.daemon.server_state import ServerAnalysisStatus
from sv.daemon.daemon_state import JobExecutionStatus
from sv.daemon.module.split_executor import FlineTaskSplitExecutor
from sv.daemon.module.http_request_client import HttpRequestError
from sv.daemon.module.update_handler import send_video_status_update
from sv.backend.service.service_manager import get_service_manager
from sv.backend.job_status import JobStatus
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class ExecutionEngine:
    """Task 실행 및 결과 처리 (스레드 관리는 ThreadManager가 담당)"""
    
    def __init__(self, base_work_dir: str, update_url: str, num_executors: int = 2):
        """
        Args:
            num_executors: Ray Actor 개수
            base_work_dir: 작업 기본 디렉토리 경로
        """
        self.num_executors = num_executors
        self.base_work_dir = base_work_dir
        self.update_url = update_url
        self.job_queue_service = get_service_manager().get_job_queue_service()

        # base_work_dir이 없으면 생성
        try:
            os.makedirs(base_work_dir, exist_ok=True)
            logger.info(f"✓ Base work directory created/exists: {base_work_dir}")
        except Exception as e:
            logger.error(f"❌ Failed to create base work directory: {str(e)}", exc_info=True)
            raise
        
        self.executor_actors = [
            FlineTaskSplitExecutor.remote(i, base_work_dir) for i in range(num_executors)
        ]
        self.current_executor_idx = 0
        
        # 비동기 작업 관리 (ray.wait() 기반)
        self.pending_jobs: Dict[int, ray.ObjectRef] = {}  # job_id -> ObjectRef
        self.job_callbacks: Dict[int, Callable] = {}  # job_id -> callback
        self.lock = Lock()
        
        logger.info(f"✓ ExecutionEngine initialized with {num_executors} executors")

    def _create_pre_secondary_callback(self, update_url: str) -> Callable:
        """
        각 데이터 아이템별로 Secondary tasks 실행 직전 데이터를 업데이트하는 콜백 생성

        Args:
            update_url: 데이터 업데이트 요청을 보낼 URL

        Returns:
            콜백 함수
        """
        def pre_secondary_callback(data_item: Dict[str, Any], loop_context: Dict[str, Any]) -> None:
            """
            각 데이터 아이템에 대해 secondary tasks 실행 직전 호출되는 콜백

            Args:
                data_item: 현재 처리 중인 데이터 아이템 (video_name, video_url 포함)
                loop_context: Job의 루프 컨텍스트 (job_id, frfr_id, analysis_id 등)
            """
            try:
                frfr_id = loop_context.get('frfr_id')
                analysis_id = loop_context.get('analysis_id')
                job_id = loop_context.get('job_id')

                # data_item에서 video_name과 video_url 추출
                video_name = data_item if isinstance(data_item, str) else data_item.get('video_name', 'unknown')
                video_url = data_item.get('video_url') if isinstance(data_item, dict) else ''

                logger.info("=" * 80)
                logger.info(f"📤 Pre-secondary callback 실행: Job {job_id}")
                logger.info(f"   URL: {update_url}")
                logger.info(f"   frfr_id: {frfr_id}, analysis_id: {analysis_id}")
                logger.info(f"   video_name: {video_name}")
                logger.info(f"   video_url: {video_url}")
                logger.info("=" * 80)

                # 파일 존재 여부 확인
                analysis_status = ServerAnalysisStatus.STAT_002
                if not os.path.isfile(video_url):
                    logger.warning(f"⚠️  파일이 존재하지 않음: {video_url}")
                    analysis_status = ServerAnalysisStatus.STAT_005                        
                    file_exists = False
                else:
                    file_exists = True
                    logger.info(f"✓ 파일 존재 확인: {video_url}")

                # 파일이 존재하면 STAT_002로 업데이트
                send_video_status_update(
                    update_url=update_url,
                    frfr_id=frfr_id,
                    analysis_id=analysis_id,
                    video_updates=[
                        {
                            "video_name": video_name,
                            "analysis_status": analysis_status.to_code()
                        }
                    ]
                )

                if not file_exists:
                    raise FileNotFoundError(f"비디오 파일을 찾을 수 없음: {video_url}")

            except HttpRequestError as e:
                logger.error(f"❌ 데이터 업데이트 요청 실패: {str(e)}", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"❌ 콜백 실행 중 예상치 못한 에러: {str(e)}", exc_info=True)
                raise

        return pre_secondary_callback
    
    def _get_next_executor(self):
        """
        라운드 로빈 방식으로 다음 Executor 반환
        
        Returns:
            Ray Actor Handle
        """
        executor = self.executor_actors[self.current_executor_idx]
        self.current_executor_idx = (self.current_executor_idx + 1) % len(self.executor_actors)
        return executor
    
    # ==================== Job 상태 관리 메서드 ====================
    
    def _update_job_status_safe(self, job_id: int, status: JobStatus, log_prefix: str = "") -> bool:
        """
        Job 상태를 안전하게 업데이트 (에러 핸들링 포함)
        
        Args:
            job_id: Job ID
            status: 변경할 상태
            log_prefix: 로그 메시지 앞에 붙일 접두사
            
        Returns:
            성공 여부
        """
        try:
            self.job_queue_service.update_job_status(job_id, status)
            logger.info(f"{log_prefix}✓ Job {job_id} status updated to {status}")
            return True
        except Exception as e:
            logger.error(f"{log_prefix}❌ Failed to update job {job_id} status to {status}: {str(e)}", exc_info=True)
            return False
    
    def _on_job_start(self, job_id: int) -> bool:
        """
        Job 시작 시 상태 업데이트
        
        Args:
            job_id: Job ID
            
        Returns:
            성공 여부
        """
        return self._update_job_status_safe(job_id, JobStatus.PROCESSING, "🚀 ")
    
    def _on_job_complete(self, job_id: int, result: Dict[str, Any]) -> None:
        """
        Job 완료 시 상태 업데이트
        
        Args:
            job_id: Job ID
            result: Job 실행 결과
        """
        status = result.get('status', 'failed')
        job_status = JobStatus.COMPLETED if status == 'success' else JobStatus.FAILED
        self._update_job_status_safe(job_id, job_status, "🏁 ")
    
    def execute_job(
        self,
        job_info: Dict[str, Any],
        primary_task: TaskBase,
        secondary_tasks: list,
        data_splitter,
        on_complete: Optional[Callable[[int, Dict[str, Any]], None]] = None
    ) -> None:
        """
        Job 실행 (비동기 콜백 방식)
        
        Args:
            job_info: Job       정보 딕셔너리
            primary_task: Primary Task
            secondary_tasks: Secondary Tasks 리스트
            data_splitter: 데이터 분할 함수
            on_complete: 완료 시 호출될 콜백 함수 (job_id, result)
        """
        job_id = job_info['job_id']
        
        # Job 시작 - 상태를 PROCESSING으로 업데이트
        if not self._on_job_start(job_id):
            logger.error("❌ Failed to update job status to PROCESSING")
            return

        try:
            if not primary_task or not secondary_tasks:
                logger.error("❌ Primary task or secondary tasks not registered")
                error_result = {
                    **job_info,
                    'status': JobExecutionStatus.FAILED.to_str(),
                    'error': 'Tasks not registered'
                }
                if on_complete:
                    on_complete(job_id, error_result)
                return
            
            logger.info("=" * 80)
            logger.info(f"🔄 Submitting job: {job_id}")
            logger.info("=" * 80)
            
            # 실행 컨텍스트 생성            
            loop_context = {
                **job_info,
                'start_time': datetime.now().strftime('%Y%m%dT%H%M%S'),
                'task_count': len(secondary_tasks)
            }
            
            executor = self._get_next_executor()
            
            logger.info(f"Submitting job {job_id} to executor with data splitting")

            # Pre-secondary 콜백 생성
            pre_secondary_callback = self._create_pre_secondary_callback(self.update_url)

            # Ray Actor에서 데이터 분할 방식 실행
            result_ref = executor.execute_with_data_splitting.remote(
                primary_task,
                secondary_tasks,
                loop_context,
                data_splitter or (lambda x: [x]),
                continue_on_error=True,
                pre_secondary_callback=pre_secondary_callback
            )
            
            # ObjectRef와 콜백을 pending_jobs에 등록
            with self.lock:
                self.pending_jobs[job_id] = result_ref
                if on_complete:
                    self.job_callbacks[job_id] = on_complete
            
            logger.info(f"✓ Job {job_id} submitted successfully (monitoring by ThreadManager)")
            
        except Exception as e:
            logger.error(f"❌ Error executing job {job_id}: {str(e)}", exc_info=True)
            error_result = {
                'job_id': job_id,
                'status': JobExecutionStatus.FAILED.to_str(),
                'error': str(e)
            }
            if on_complete:
                on_complete(job_id, error_result)
    
    # ==================== 모니터링 인터페이스 (ThreadManager가 호출) ====================
    
    def get_pending_jobs_snapshot(self) -> Dict[int, ray.ObjectRef]:
        """
        현재 pending jobs의 스냅샷 반환 (ThreadManager의 모니터 스레드에서 사용)
        
        Returns:
            {job_id: ObjectRef} 딕셔너리
        """
        with self.lock:
            return dict(self.pending_jobs)
    
    def check_and_process_completed_jobs(self, timeout: float = 1.0) -> List[Tuple[int, Dict[str, Any]]]:
        """
        완료된 작업을 확인하고 처리 (ThreadManager의 모니터 스레드에서 호출)
        
        Args:
            timeout: ray.wait() 타임아웃 (초)
            
        Returns:
            완료된 작업 리스트 [(job_id, result), ...]
        """
        with self.lock:
            if not self.pending_jobs:
                return []
            
            job_refs_map = dict(self.pending_jobs)
        
        if not job_refs_map:
            return []
        
        completed_jobs = []
        
        try:
            # ray.wait()로 완료된 작업 확인
            object_refs = list(job_refs_map.values())
            ready_refs, _ = ray.wait(
                object_refs,
                num_returns=len(object_refs),
                timeout=timeout
            )
            
            # ready된 작업들 처리
            for ready_ref in ready_refs:
                # ObjectRef에 해당하는 job_id 찾기
                completed_job_id = None
                for jid, ref in job_refs_map.items():
                    if ref == ready_ref:
                        completed_job_id = jid
                        break
                
                if completed_job_id is None:
                    continue
                
                try:
                    # 결과 가져오기
                    result = ray.get(ready_ref)
                    
                    logger.info("=" * 80)
                    logger.info(f"✅ Job {completed_job_id} execution completed: {result.get('status')}")
                    logger.info("=" * 80)

                    # Job 완료 - 상태를 COMPLETED/FAILED로 업데이트
                    self._on_job_complete(completed_job_id, result)

                    # 콜백 호출
                    with self.lock:
                        callback = self.job_callbacks.get(completed_job_id)
                    
                    if callback:
                        callback(completed_job_id, result)
                    
                    completed_jobs.append((completed_job_id, result))
                    
                except Exception as e:
                    logger.error(f"❌ Error processing job {completed_job_id}: {str(e)}", exc_info=True)
                    error_result = {
                        'job_id': completed_job_id,
                        'status': JobExecutionStatus.FAILED.to_str(),
                        'error': str(e)
                    }
                    
                    with self.lock:
                        callback = self.job_callbacks.get(completed_job_id)
                    
                    if callback:
                        callback(completed_job_id, error_result)
                    
                    completed_jobs.append((completed_job_id, error_result))
                
                finally:
                    # pending_jobs에서 제거
                    with self.lock:
                        self.pending_jobs.pop(completed_job_id, None)
                        self.job_callbacks.pop(completed_job_id, None)
            
        except Exception as e:
            logger.error(f"Error checking completed jobs: {str(e)}", exc_info=True)
        
        return completed_jobs
    
    def log_execution_result(self, job_id: int, result: Dict[str, Any]) -> None:
        """
        실행 결과 로깅
        
        Args:
            job_id: Job ID
            result: 실행 결과
        """
        logger.info("📊 Execution Result:")
        logger.info(f"  Job ID: {job_id}")
        logger.info(f"  Status: {result.get('status')}")
        logger.info(f"  Duration: {result.get('total_duration', 0):.2f}s")
        logger.info(f"  Data items: {len(result.get('data_items', []))}")
        logger.info(f"  Errors: {result.get('error_count', 0)}")
        
        # Primary Task 결과
        if result.get('primary_task'):
            primary = result['primary_task']
            logger.info(f"  Primary Task: {primary['status']} ({primary.get('duration', 0):.2f}s)")
        
        # 각 데이터 아이템 처리 결과
        for item_idx, item_result in enumerate(result.get('data_items', []), 1):
            logger.info(f"  Item {item_idx}: {item_result['status']} ({item_result.get('duration', 0):.2f}s)")
            for task_info in item_result.get('tasks', []):
                status_icon = "✓" if task_info['status'] == 'success' else "✗"
                logger.info(
                    f"    {status_icon} {task_info['task_name']}: "
                    f"{task_info['status']} ({task_info.get('duration', 0):.2f}s)"
                )
    
    def get_executor_status(self) -> Dict[str, Any]:
        """
        Executor 상태 반환
        
        Returns:
            Executor 상태 정보
        """
        with self.lock:
            pending_count = len(self.pending_jobs)
        
        return {
            'num_executors': self.num_executors,
            'current_executor_idx': self.current_executor_idx,
            'pending_jobs_count': pending_count
        }

