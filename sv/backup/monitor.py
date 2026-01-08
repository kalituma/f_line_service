import ray
from typing import Dict, Any
from datetime import datetime
import requests

from sv.backup.executor import FlineTaskExecutor, execute_task
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class FlineTaskMonitor:
    """Ray를 사용한 작업 모니터"""

    def __init__(self, num_executors=5, use_actors=True):
        self.use_actors = use_actors
        self.active_tasks = {}

        if use_actors:
            # Actor 풀 생성
            self.executors = [
                FlineTaskExecutor.remote(i) for i in range(num_executors)
            ]
            self.current_executor = 0
        else:
            self.executors = None

    def submit_task(self, task_data: Dict[str, Any]):
        """작업 제출"""
        task_id = task_data.get('task_id')

        # 중복 실행 방지
        if task_id in self.active_tasks:
            logger.warning(f"Task {task_id} already running, skipping")
            return

        if self.use_actors:
            # Actor 풀에서 라운드 로빈 방식으로 선택
            executor = self.executors[self.current_executor]
            self.current_executor = (self.current_executor + 1) % len(self.executors)
            object_ref = executor.execute.remote(task_data)
        else:
            # Stateless 함수 사용
            object_ref = execute_task.remote(task_data)

        self.active_tasks[task_id] = {
            'object_ref': object_ref,
            'submitted_at': datetime.now().isoformat(),
            'task_data': task_data
        }

        logger.info(f"Task {task_id} submitted. Active: {len(self.active_tasks)}")

    def check_completed_tasks(self):
        """완료된 작업 확인 및 콜백 실행"""
        completed_tasks = []

        for task_id, task_info in list(self.active_tasks.items()):
            object_ref = task_info['object_ref']

            # non-blocking으로 완료 여부 확인
            ready_refs, _ = ray.wait([object_ref], timeout=0)

            if ready_refs:
                # 작업 완료
                try:
                    result = ray.get(object_ref)
                    completed_tasks.append(task_id)

                    # 콜백 실행
                    if result['status'] == 'success':
                        self._handle_success(result)
                    else:
                        self._handle_failure(result)

                except Exception as e:
                    logger.error(f"Error getting result for {task_id}: {str(e)}")
                    completed_tasks.append(task_id)

        # 완료된 작업 제거
        for task_id in completed_tasks:
            del self.active_tasks[task_id]

        if completed_tasks:
            logger.info(f"Completed tasks: {completed_tasks}")

    def _handle_success(self, result: Dict[str, Any]):
        """성공 콜백"""
        logger.info(f"Success callback for task {result['task_id']}")

        try:
            response = requests.post(
                'https://your-api.com/task-results',
                json=result,
                timeout=10
            )
            logger.info(f"Result sent: {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to send result: {str(e)}")

    def _handle_failure(self, result: Dict[str, Any]):
        """실패 콜백"""
        logger.warning(f"Failure callback for task {result['task_id']}")

        try:
            requests.post(
                'https://your-api.com/task-failures',
                json=result,
                timeout=10
            )
        except Exception as e:
            logger.error(f"Failed to report failure: {str(e)}")

    def wait_all(self, timeout=None):
        """모든 작업 완료 대기"""
        if not self.active_tasks:
            return

        object_refs = [info['object_ref'] for info in self.active_tasks.values()]

        try:
            results = ray.get(object_refs, timeout=timeout)

            for result in results:
                if result['status'] == 'success':
                    self._handle_success(result)
                else:
                    self._handle_failure(result)

        except Exception as e:
            logger.error(f"Error waiting for tasks: {str(e)}")
        finally:
            self.active_tasks.clear()

    def get_status(self) -> Dict[str, Any]:
        """현재 상태"""
        return {
            'active_tasks': len(self.active_tasks),
            'task_ids': list(self.active_tasks.keys()),
            'use_actors': self.use_actors
        }

    def shutdown(self):
        """종료"""
        logger.info("Shutting down Ray task monitor...")
        self.wait_all(timeout=30)