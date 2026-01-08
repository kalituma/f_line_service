import asyncio
from typing import List, Callable
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class RecoveryCheckService:
    """
    서버 초기화 및 복구 체크를 관리하는 서비스
    
    여러 초기화 단계(daemon 실행, DB 체크 등)를 순차적으로 실행합니다.
    """
    
    def __init__(self):
        """서비스 초기화"""
        self._tasks: List[tuple[str, Callable]] = []
    
    def add_task(self, name: str, coro_func: Callable) -> None:
        """
        초기화 작업을 추가합니다.
        
        Args:
            name: 작업 이름 (로깅용)
            coro_func: 실행할 코루틴 함수 (async def)
        """
        self._tasks.append((name, coro_func))
        logger.info(f"Added initialization task: {name}")
    
    async def run_all(self) -> bool:
        """
        등록된 모든 초기화 작업을 순차적으로 실행합니다.
        
        Returns:
            bool: 모든 작업 성공 여부
        """
        logger.info(f"Starting {len(self._tasks)} initialization tasks...")
        
        for task_name, coro_func in self._tasks:
            try:
                logger.info(f"Running task: {task_name}")
                await coro_func()
                logger.info(f"✓ Task completed: {task_name}")
            except Exception as e:
                logger.error(f"✗ Task failed: {task_name} - {str(e)}")
                return False
        
        logger.info("✓ All initialization tasks completed successfully!")
        return True

