import ray
import logging
from datetime import datetime
from typing import Dict, Any, List
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ray 초기화
ray.init(num_cpus=8)  # CPU 코어 수에 맞게 조정

@ray.remote
class FlineTaskExecutor:
    """Ray Actor로 작업을 실행하는 클래스"""

    def __init__(self, executor_id: int):
        self.executor_id = executor_id
        self.logger = logging.getLogger(f"Executor-{executor_id}")

    def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """실제 작업 수행"""
        task_id = task_data.get('task_id', 'unknown')
        self.logger.info(f"Executor {self.executor_id} processing task {task_id}")

        try:
            # 실제 작업 로직
            result = self._do_work(task_data)

            self.logger.info(f"Task {task_id} completed")
            return {
                'task_id': task_id,
                'executor_id': self.executor_id,
                'status': 'success',
                'result': result,
                'completed_at': datetime.now().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Task {task_id} failed: {str(e)}")
            return {
                'task_id': task_id,
                'executor_id': self.executor_id,
                'status': 'failed',
                'error': str(e),
                'completed_at': datetime.now().isoformat()
            }

    def _do_work(self, task_data: Dict[str, Any]) -> Any:
        """비즈니스 로직"""
        # 예시: 시간이 걸리는 작업
        time.sleep(5)
        return {"processed": True, "data": task_data}


@ray.remote
def execute_task(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """Ray 원격 함수로 작업 실행 (stateless 방식)"""
    task_id = task_data.get('task_id', 'unknown')
    logger.info(f"Processing task {task_id}")

    try:
        # 실제 작업
        time.sleep(5)  # 실제 작업으로 대체
        result = {"processed": True, "data": task_data}

        return {
            'task_id': task_id,
            'status': 'success',
            'result': result,
            'completed_at': datetime.now().isoformat()
        }

    except Exception as e:
        return {
            'task_id': task_id,
            'status': 'failed',
            'error': str(e),
            'completed_at': datetime.now().isoformat()
        }