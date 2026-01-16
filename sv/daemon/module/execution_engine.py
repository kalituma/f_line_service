from typing import Dict, Any, Callable, Optional, Tuple, List
from datetime import datetime
from threading import Lock
import os

try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore


from sv.daemon.daemon_state import JobExecutionStatus
from sv.daemon.module.split_executor import FlineTaskSplitExecutor
from sv.daemon.module.fault_injector import FaultInjector
from sv.backend.service.service_manager import get_service_manager
from sv.backend.work_status import WorkStatus
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
        self._work_queue_service = None

        # base_work_dir이 없으면 생성
        try:
            os.makedirs(base_work_dir, exist_ok=True)
            logger.info(f"✓ Base work directory created/exists: {base_work_dir}")
        except Exception as e:
            logger.error(f"❌ Failed to create base work directory: {str(e)}", exc_info=True)
            raise

        fault_injector = FaultInjector()
    
    
        # fault_injector.register_fault(
        #     method_name="_on_secondary_tasks_start",
        #     exception=FileNotFoundError("raised intentional file not found error"),
        #     probability=1.0
        # )
        self.executor_actors = [
            FlineTaskSplitExecutor.remote(i, base_work_dir, update_url, fault_injector) for i in range(num_executors)
        ]
        self.current_executor_idx = 0
        
        # 비동기 작업 관리 (ray.wait() 기반)
        self.pending_works: Dict[int, ray.ObjectRef] = {}  # work_id -> ObjectRef
        self.work_callbacks: Dict[int, Callable] = {}  # work_id -> callback
        self.lock = Lock()
        
        logger.info(f"✓ ExecutionEngine initialized with {num_executors} executors")

    @property
    def work_queue_service(self):
        if self._work_queue_service is None:
            self._work_queue_service = get_service_manager().get_work_queue_service()
        return self._work_queue_service

    def _get_next_executor(self):
        """
        라운드 로빈 방식으로 다음 Executor 반환
        
        Returns:
            Ray Actor Handle
        """
        executor = self.executor_actors[self.current_executor_idx]
        self.current_executor_idx = (self.current_executor_idx + 1) % len(self.executor_actors)
        return executor
    
    # ==================== Work 상태 관리 메서드 ====================
    
    def _update_work_status_safe(self, work_id: int, status: WorkStatus) -> bool:
        """
        Work 상태를 안전하게 업데이트 (에러 핸들링 포함)
        
        Args:
            work_id: Work ID
            status: 변경할 상태
            log_prefix: 로그 메시지 앞에 붙일 접두사
            
        Returns:
            성공 여부
        """
        try:
            self.work_queue_service.update_work_status(work_id, status)
            logger.info(f"✓ Work {work_id} status updated to {status}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to update work {work_id} status to {status}: {str(e)}", exc_info=True)
            return False
    
    def _on_work_start(self, work_id: int) -> bool:
        """
        Work 시작 시 상태 업데이트
        
        Args:
            work_id: Work ID
            
        Returns:
            성공 여부
        """
        return self._update_work_status_safe(work_id, WorkStatus.PROCESSING)
    
    def _on_execution_complete(self, work_id: int, result: Dict[str, Any]) -> None:
        """
        Work 완료 시 상태 업데이트
        
        Args:
            work_id: Work ID
            result: Work 실행 결과
        """
        status = result.get('status', 'failed')
        error_count = result.get('error_count', 0)
        
        # status가 'success'이고 error_count가 0일 때만 COMPLETED
        # 하나라도 실패하면 FAILED로 처리
        if status == 'success' and error_count == 0:
            work_status = WorkStatus.COMPLETED
            logger.info(f"Work {work_id} completed successfully (status={status}, errors={error_count})")
        else:
            work_status = WorkStatus.FAILED
            logger.warning(f"Work {work_id} marked as FAILED (status={status}, errors={error_count})")
            
        self._update_work_status_safe(work_id, work_status)
    
    def execute_work(
        self,
        work_info: Dict[str, Any],
        primary_task: TaskBase,
        secondary_tasks: list,
        data_splitter,
        on_job_complete: Optional[Callable[[int, Dict[str, Any]], None]] = None
    ) -> None:
        """
        Work 실행 (비동기 콜백 방식)
        
        Args:
            work_info: Work       정보 딕셔너리
            primary_task: Primary Task
            secondary_tasks: Secondary Tasks 리스트
            data_splitter: 데이터 분할 함수
            on_job_complete: 완료 시 호출될 콜백 함수 (work_id, result)
        """
        work_id = work_info['work_id']
        
        # Work 시작 - 상태를 PROCESSING으로 업데이트
        if not self._on_work_start(work_id):
            logger.error("❌ Failed to update work status to PROCESSING")
            return

        try:
            if not primary_task or not secondary_tasks:
                logger.error("❌ Primary task or secondary tasks not registered")
                self._update_work_status_safe(work_id, WorkStatus.FAILED)

                error_result = {
                    **work_info,
                    'status': JobExecutionStatus.FAILED.to_str(),
                    'error': 'Tasks not registered'
                }
                if on_job_complete:
                    on_job_complete(work_id, error_result)
                return
            
            logger.info("=" * 80)
            logger.info(f"🔄 Submitting work: {work_id}")
            logger.info("=" * 80)
            
            # 실행 컨텍스트 생성            
            loop_context = {
                **work_info,
                'start_time': datetime.now().strftime('%Y%m%dT%H%M%S'),
                'task_count': len(secondary_tasks)
            }
            
            executor = self._get_next_executor()
            
            logger.info(f"Submitting work {work_id} to executor with data splitting")

            result_ref = executor.execute_with_data_splitting.remote(
                primary_task,
                secondary_tasks,
                loop_context,
                data_splitter or (lambda x: [x]),
                continue_on_error=True,
            )
            
            # ObjectRef와 콜백을 pending_works에 등록
            with self.lock:
                self.pending_works[work_id] = result_ref
                if on_job_complete:
                    self.work_callbacks[work_id] = on_job_complete
            
            logger.info(f"✓ Work {work_id} submitted successfully (monitoring by ThreadManager)")
            
        except Exception as e:
            logger.error(f"❌ Error executing work {work_id}: {str(e)}", exc_info=True)            
            self._update_work_status_safe(work_id, WorkStatus.FAILED)
            error_result = {
                'work_id': work_id,
                'status': JobExecutionStatus.FAILED.to_str(),
                'error': str(e)
            }
            if on_job_complete:
                on_job_complete(work_id, error_result)
    
    # ==================== 모니터링 인터페이스 (ThreadManager가 호출) ====================
    
    def get_pending_works_snapshot(self) -> Dict[int, ray.ObjectRef]:
        """
        현재 pending works의 스냅샷 반환 (ThreadManager의 모니터 스레드에서 사용)
        
        Returns:
            {work_id: ObjectRef} 딕셔너리
        """
        with self.lock:
            return dict(self.pending_works)
    
    def check_and_process_completed_works(self, timeout: float = 1.0) -> List[Tuple[int, Dict[str, Any]]]:
        """
        완료된 작업을 확인하고 처리 (ThreadManager의 모니터 스레드에서 호출)
        
        Args:
            timeout: ray.wait() 타임아웃 (초)
            
        Returns:
            완료된 작업 리스트 [(work_id, result), ...]
        """
        with self.lock:
            if not self.pending_works:
                return []
            
            work_refs_map = dict(self.pending_works)
        
        if not work_refs_map:
            return []
        
        completed_works = []
        
        try:
            # ray.wait()로 완료된 작업 확인
            object_refs = list(work_refs_map.values())
            ready_refs, _ = ray.wait(
                object_refs,
                num_returns=len(object_refs),
                timeout=timeout
            )
            
            # 역방향 매핑 생성 (ObjectRef -> work_id)
            ref_to_work_id = {ref: wid for wid, ref in work_refs_map.items()}
            
            # ready된 작업들 처리
            for ready_ref in ready_refs:
                # ObjectRef에 해당하는 work_id 찾기
                completed_work_id = ref_to_work_id.get(ready_ref)
                
                if completed_work_id is None:
                    # 이론적으로 불가능하지만, Ray 내부 버그를 대비한 방어 코드
                    logger.error(
                        f"🐛 UNEXPECTED: Could not find work_id for completed ref: {ready_ref}. "
                        f"This should never happen. Possible Ray bug."
                    )
                    try:
                        ray.get(ready_ref, timeout=0)
                    except Exception as cleanup_error:
                        logger.debug(f"Error cleaning up orphaned ref: {cleanup_error}")
                    continue
                
                try:
                    # 결과 가져오기
                    result = ray.get(ready_ref)
                    
                    logger.info("=" * 80)
                    logger.info(f"✅ Work {completed_work_id} execution completed: {result.get('status')}")
                    logger.info("=" * 80)

                    # Work 상태 업데이트 (콜백 실패와 무관하게 유지되어야 함)
                    self._on_execution_complete(completed_work_id, result)
                    
                    completed_works.append((completed_work_id, result))
                    
                    # 콜백 호출 (예외가 발생해도 Work 상태에 영향 없도록 별도 처리)
                    with self.lock:
                        callback = self.work_callbacks.get(completed_work_id)
                    
                    if callback:
                        try:
                            callback(completed_work_id, result)
                        except Exception as callback_error:
                            logger.error(
                                f"⚠️ Callback error for work {completed_work_id} "
                                f"(Work itself succeeded): {callback_error}",
                                exc_info=True
                            )
                    
                except Exception as e:
                    logger.error(f"❌ Error processing work {completed_work_id}: {str(e)}", exc_info=True)
                    self._update_work_status_safe(completed_work_id, WorkStatus.FAILED)
                    error_result = {
                        'work_id': completed_work_id,
                        'status': JobExecutionStatus.FAILED.to_str(),
                        'error': str(e)
                    }
                    
                    completed_works.append((completed_work_id, error_result))
                    
                    # 콜백 호출 (안전하게 처리)
                    with self.lock:
                        callback = self.work_callbacks.get(completed_work_id)
                    
                    if callback:
                        try:
                            callback(completed_work_id, error_result)
                        except Exception as callback_error:
                            logger.error(
                                f"⚠️ Callback error for work {completed_work_id}: {callback_error}",
                                exc_info=True
                            )
                
                finally:
                    # pending_works에서 제거
                    with self.lock:
                        self.pending_works.pop(completed_work_id, None)
                        self.work_callbacks.pop(completed_work_id, None)
            
        except Exception as e:
            logger.error(f"💥 Critical error in ray.wait(): {str(e)}", exc_info=True)
            # ray.wait() 자체가 실패한 경우, 모든 pending works를 FAILED로 처리
            for work_id in list(work_refs_map.keys()):
                logger.error(f"❌ Marking work {work_id} as FAILED due to ray.wait() error")
                self._update_work_status_safe(work_id, WorkStatus.FAILED)
                
                error_result = {
                    'work_id': work_id,
                    'status': JobExecutionStatus.FAILED.to_str(),
                    'error': f'Ray wait error: {str(e)}'
                }
                
                # 콜백 호출
                with self.lock:
                    callback = self.work_callbacks.get(work_id)
                
                if callback:
                    try:
                        callback(work_id, error_result)
                    except Exception as callback_error:
                        logger.error(f"Error calling callback for work {work_id}: {callback_error}")
                
                completed_works.append((work_id, error_result))
                
                # pending_works에서 제거
                with self.lock:
                    self.pending_works.pop(work_id, None)
                    self.work_callbacks.pop(work_id, None)
        
        return completed_works
    
    def log_execution_result(self, work_id: int, result: Dict[str, Any]) -> None:
        """
        실행 결과 로깅
        
        Args:
            work_id: Work ID
            result: 실행 결과
        """
        logger.info("📊 Execution Result:")
        logger.info(f"  Work ID: {work_id}")
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
            pending_count = len(self.pending_works)
        
        return {
            'num_executors': self.num_executors,
            'current_executor_idx': self.current_executor_idx,
            'pending_works_count': pending_count
        }

