from typing import List, Dict, Any, Optional, Callable
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)


class TaskManager:
    """Task 생명주기 관리"""
    
    def __init__(self):
        self.primary_task: Optional[TaskBase] = None
        self.secondary_tasks: List[TaskBase] = []
        self.data_splitter: Optional[Callable[[Dict[str, Any]], List[Any]]] = None
    
    def register_primary_task(self, task: TaskBase) -> None:
        """
        Primary Task 등록 (Task 1)
        
        Args:
            task: 등록할 primary task
        """
        self.primary_task = task
        logger.info(f"✓ Primary task registered: {task.task_name}")
    
    def register_secondary_tasks(self, tasks: List[TaskBase]) -> None:
        """
        Secondary Tasks 등록 (Task 2~N)
        
        Args:
            tasks: 등록할 secondary task 리스트
        """
        self.secondary_tasks.extend(tasks)
        task_names = [t.task_name for t in tasks]
        logger.info(f"✓ {len(tasks)} secondary tasks registered: {task_names}")
    
    def set_data_splitter(self, splitter: Callable[[Dict[str, Any]], List[Any]]) -> None:
        """
        데이터 분할 함수 등록
        
        Args:
            splitter: Primary Task 결과를 분할하는 함수
        """
        self.data_splitter = splitter
        logger.info(f"✓ Data splitter registered: {splitter.__name__}")
    
    def are_tasks_ready(self) -> bool:
        """
        Task가 모두 등록되었는지 확인
        
        Returns:
            Primary + Secondary Tasks가 모두 등록되었으면 True
        """
        return self.primary_task is not None and len(self.secondary_tasks) > 0
    
    def get_tasks_summary(self) -> Dict[str, Any]:
        """
        Task 등록 상태 요약
        
        Returns:
            Task 정보 딕셔너리
        """
        return {
            'primary_task': self.primary_task.task_name if self.primary_task else None,
            'secondary_tasks_count': len(self.secondary_tasks),
            'has_data_splitter': self.data_splitter is not None,
            'tasks_ready': self.are_tasks_ready()
        }

