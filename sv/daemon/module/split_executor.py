try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

import logging
from datetime import datetime
from typing import Dict, Any, List, Callable, Optional

from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger
logger = setup_logger(__name__)

@ray.remote
class FlineTaskSplitExecutor:
    """Ray Actor로 순차 작업을 실행하는 클래스 (데이터 분할 지원)"""

    def __init__(self, executor_id: int):
        self.executor_id = executor_id
        self.logger = logging.getLogger(f"Executor-{executor_id}")

    def execute_sequential_tasks(
        self,
        tasks: List[TaskBase],
        loop_context: Dict[str, Any],
        data_processor: Optional[Callable[[Dict[str, Any]], Any]] = None
    ) -> Dict[str, Any]:
        """
        순차적으로 여러 작업을 실행합니다.
        
        Args:
            tasks: 실행할 작업 목록 (TaskBase 인스턴스)
            loop_context: 루프의 컨텍스트 정보
            data_processor: 첫 번째 Task 결과를 처리할 함수 (데이터 분할 등)
            
        Returns:
            전체 실행 결과
            
        Example:
            ```python
            def split_data(result):
                # Task 1의 결과가 리스트라면 개별 아이템 반환
                return result.get('items', [])
            
            result = executor.execute_sequential_tasks(
                tasks=[task1, task2, task3],
                loop_context={'job_id': 123},
                data_processor=split_data
            )
            ```
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

    def execute_with_data_splitting(
        self,
        primary_task: TaskBase,
        secondary_tasks: List[TaskBase],
        loop_context: Dict[str, Any],
        data_splitter: Callable[[Any], List[Any]],
        continue_on_error: bool = True
    ) -> Dict[str, Any]:
        """
        Task 1을 실행 후 결과를 분할하여 각 데이터에 대해 순차 Task들을 실행
        
        Args:
            primary_task: 먼저 실행할 주요 작업 (Task 1)
            secondary_tasks: 분할된 각 데이터에 대해 실행할 작업 리스트
            loop_context: 루프의 컨텍스트 정보
            data_splitter: 주요 작업 결과를 리스트로 분할하는 함수
            continue_on_error: 에러 발생 시 계속 실행할지 여부
            
        Returns:
            전체 실행 결과
            
        Example:
            ```python
            def split_result(result):
                # result['data']가 리스트라면 각 아이템 반환
                return result.get('data', [])
            
            result = executor.execute_with_data_splitting(
                primary_task=task1,
                secondary_tasks=[task2, task3],
                loop_context={'job_id': 123},
                data_splitter=split_result
            )
            ```
        """
        self.logger.info(
            f"Executor {self.executor_id} executing with data splitting: "
            f"primary={primary_task.task_name}, secondary={[t.task_name for t in secondary_tasks]}"
        )
        
        results = {
            'executor_id': self.executor_id,
            'loop_context': loop_context,
            'primary_task': None,
            'data_items': [],
            'status': 'success',
            'completed_at': datetime.now().isoformat(),
            'total_duration': 0,
            'error_count': 0
        }
        
        start_time = datetime.now()
        task_context = {}
        
        # ==================== Step 1: 주요 작업 실행 ====================
        try:
            self.logger.info(f"[1] Executing primary task: {primary_task.task_name}")
            
            primary_start = datetime.now()
            primary_result = primary_task.execute(task_context)
            primary_duration = (datetime.now() - primary_start).total_seconds()
            
            task_context[primary_task.task_name] = primary_result
            
            self.logger.info(
                f"[1] Primary task '{primary_task.task_name}' completed ({primary_duration:.2f}s)"
            )
            
            results['primary_task'] = {
                'task_name': primary_task.task_name,
                'status': 'success',
                'result': primary_result,
                'duration': primary_duration
            }
        
        except Exception as e:
            error_msg = f"Primary task '{primary_task.task_name}' failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            results['status'] = 'failed'
            results['primary_task'] = {
                'task_name': primary_task.task_name,
                'status': 'failed',
                'error': str(e)
            }
            
            total_duration = (datetime.now() - start_time).total_seconds()
            results['total_duration'] = total_duration
            
            return results
        
        # ==================== Step 2: 데이터 분할 ====================
        try:
            self.logger.info("[2] Splitting data from primary task result")
            
            data_items = data_splitter(primary_result)
            
            if not isinstance(data_items, list):
                data_items = [data_items]
            
            self.logger.info(
                f"[2] Data split into {len(data_items)} items"
            )
            
        except Exception as e:
            error_msg = f"Data splitting failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            results['status'] = 'failed'
            results['error'] = error_msg
            
            total_duration = (datetime.now() - start_time).total_seconds()
            results['total_duration'] = total_duration
            
            return results
        
        # ==================== Step 3: 각 데이터에 대해 순차 작업 실행 ====================
        for data_idx, data_item in enumerate(data_items, 1):
            self.logger.info(f"[3.{data_idx}/{len(data_items)}] Processing data item: {data_item}")
            
            item_context = task_context.copy()
            item_context['current_data'] = data_item
            item_context['data_index'] = data_idx - 1
            item_context['total_data_items'] = len(data_items)
            
            item_result = {
                'data_item': data_item,
                'data_index': data_idx - 1,
                'tasks': [],
                'status': 'success'
            }
            
            item_start = datetime.now()
            
            # 각 데이터에 대해 모든 secondary task 실행
            for task_seq, task in enumerate(secondary_tasks, 1):
                try:
                    self.logger.info(
                        f"[3.{data_idx}/{len(data_items)}.{task_seq}/{len(secondary_tasks)}] "
                        f"Executing task: {task.task_name}"
                    )
                    
                    task_start = datetime.now()
                    task_result = task.execute(item_context)
                    task_duration = (datetime.now() - task_start).total_seconds()
                    
                    # 컨텍스트 업데이트
                    item_context[task.task_name] = task_result
                    
                    self.logger.info(
                        f"[3.{data_idx}/{len(data_items)}.{task_seq}/{len(secondary_tasks)}] "
                        f"Task '{task.task_name}' completed ({task_duration:.2f}s)"
                    )
                    
                    item_result['tasks'].append({
                        'task_name': task.task_name,
                        'status': 'success',
                        'result': task_result,
                        'duration': task_duration
                    })
                
                except Exception as e:
                    error_msg = f"Task '{task.task_name}' failed: {str(e)}"
                    self.logger.error(error_msg, exc_info=True)
                    
                    item_result['status'] = 'failed'
                    item_result['tasks'].append({
                        'task_name': task.task_name,
                        'status': 'failed',
                        'error': str(e)
                    })
                    
                    results['error_count'] += 1
                    
                    if not continue_on_error:
                        break
            
            item_result['duration'] = (datetime.now() - item_start).total_seconds()
            results['data_items'].append(item_result)
        
        total_duration = (datetime.now() - start_time).total_seconds()
        results['total_duration'] = total_duration
        
        self.logger.info(
            f"Data splitting task execution completed "
            f"(Status: {results['status']}, Total: {total_duration:.2f}s, "
            f"Errors: {results['error_count']})"  # noqa: F541
        )
        
        return results

