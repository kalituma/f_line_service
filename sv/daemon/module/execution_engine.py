"""
Task 실행 엔진 담당 클래스
Ray Actor를 통한 Task 실행 및 결과 처리
"""

from typing import Dict, Any
from datetime import datetime

try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

from sv.daemon.module.split_executor import FlineTaskSplitExecutor
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class ExecutionEngine:
    """Task 실행 및 결과 처리"""
    
    def __init__(self, num_executors: int = 2):
        """
        Args:
            num_executors: Ray Actor 개수
        """
        self.num_executors = num_executors
        self.executor_actors = [
            FlineTaskSplitExecutor.remote(i) for i in range(num_executors)
        ]
        self.current_executor_idx = 0
        logger.info(f"✓ ExecutionEngine initialized with {num_executors} executors")
    
    def _get_next_executor(self):
        """
        라운드 로빈 방식으로 다음 Executor 반환
        
        Returns:
            Ray Actor Handle
        """
        executor = self.executor_actors[self.current_executor_idx]
        self.current_executor_idx = (self.current_executor_idx + 1) % len(self.executor_actors)
        return executor
    
    async def execute_job(
        self,
        job_id: int,
        primary_task: TaskBase,
        secondary_tasks: list,
        data_splitter
    ) -> Dict[str, Any]:
        """
        Job 실행
        
        Args:
            job_id: Job ID
            primary_task: Primary Task
            secondary_tasks: Secondary Tasks 리스트
            data_splitter: 데이터 분할 함수
            
        Returns:
            실행 결과
        """
        try:
            if not primary_task or not secondary_tasks:
                logger.error("❌ Primary task or secondary tasks not registered")
                return {
                    'job_id': job_id,
                    'status': 'failed',
                    'error': 'Tasks not registered'
                }
            
            logger.info("=" * 80)
            logger.info(f"🔄 Processing job: {job_id}")
            logger.info("=" * 80)
            
            # 실행 컨텍스트 생성
            loop_context = {
                'job_id': job_id,
                'start_time': datetime.now().isoformat(),
                'task_count': len(secondary_tasks)
            }
            
            executor = self._get_next_executor()
            
            logger.info(f"Submitting job {job_id} to executor with data splitting")
            
            # Ray Actor에서 데이터 분할 방식 실행
            result_ref = executor.execute_with_data_splitting.remote(
                primary_task,
                secondary_tasks,
                loop_context,
                data_splitter or (lambda x: [x]),
                continue_on_error=True
            )
            
            # 결과 대기
            result = ray.get(result_ref)
            
            logger.info("=" * 80)
            logger.info(f"✅ Job {job_id} execution completed: {result.get('status')}")
            logger.info("=" * 80)
            
            return result
        
        except Exception as e:
            logger.error(f"❌ Error executing job {job_id}: {str(e)}", exc_info=True)
            return {
                'job_id': job_id,
                'status': 'failed',
                'error': str(e)
            }
    
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
        return {
            'num_executors': self.num_executors,
            'current_executor_idx': self.current_executor_idx
        }

