import ray
import logging
from datetime import datetime
from typing import Dict, Any, List, Callable
import time

from sv.task.task_base import TaskBase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ray 초기화는 메인 스크립트에서 수행하세요
# 예: ray.init(num_cpus=8, ignore_reinit_error=True)

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

    def execute_sequential_tasks(
        self,
        tasks: List[TaskBase],
        loop_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """순차적으로 여러 작업을 실행합니다.
        
        Args:
            tasks: 실행할 작업 목록 (TaskBase 인스턴스)
            loop_context: 루프의 컨텍스트 정보
            
        Returns:
            전체 실행 결과
        """
        self.logger.info(f"Executor {self.executor_id} executing {len(tasks)} sequential tasks")
        self.logger.info(f"Loop context: {loop_context}")
        
        results = {
            'executor_id': self.executor_id,
            'loop_context': loop_context,
            'tasks': [],
            'status': 'success',
            'completed_at': datetime.now().isoformat(),
            'total_duration': 0
        }
        
        start_time = datetime.now()
        task_context = {}  # 작업들 간 컨텍스트 공유
        
        for idx, task in enumerate(tasks, 1):
            try:
                self.logger.info(f"[{idx}/{len(tasks)}] Executing task: {task.task_name}")
                
                task_start = datetime.now()
                task_result = task.execute(task_context)
                task_duration = (datetime.now() - task_start).total_seconds()
                
                # 작업 결과를 컨텍스트에 추가
                task_context[task.task_name] = task_result
                
                self.logger.info(
                    f"[{idx}/{len(tasks)}] Task '{task.task_name}' completed "
                    f"({task_duration:.2f}s)"
                )
                
                results['tasks'].append({
                    'task_name': task.task_name,
                    'status': 'success',
                    'result': task_result,
                    'duration': task_duration
                })
            
            except Exception as e:
                error_msg = f"Task '{task.task_name}' failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                
                results['status'] = 'failed'
                results['tasks'].append({
                    'task_name': task.task_name,
                    'status': 'failed',
                    'error': str(e)
                })
                
                # 에러 발생 시 다음 작업은 계속 실행할지 선택
                # 여기서는 계속 실행하도록 설정 (필요시 break로 중단)
                continue
        
        total_duration = (datetime.now() - start_time).total_seconds()
        results['total_duration'] = total_duration
        results['task_context'] = task_context
        
        self.logger.info(
            f"Sequential task execution completed "
            f"(Status: {results['status']}, Total: {total_duration:.2f}s)"
        )
        
        return results


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