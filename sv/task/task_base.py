import logging
import time
from typing import Dict, Any, Optional

from sv.utils.logger import setup_logger



class TaskBase:
    """작업의 기본 클래스 (Template Method 패턴)"""

    def __init__(
        self, 
        task_name: str,
        delay_seconds: Optional[float] = None,
        raise_exception: Optional[Exception] = None
    ):
        """
        Args:
            task_name: 작업 이름
            delay_seconds: 작업 실행 시 지연 시간(초). None이면 지연 없음
            raise_exception: 작업 실행 시 발생시킬 예외. None이면 정상 실행
        """
        self.task_name = task_name
        self.logger = setup_logger(f"Task-{task_name}")
        self.delay_seconds = delay_seconds
        self.raise_exception = raise_exception
        self.job_queue_service = None
        
    def before_execute(self, context: Dict[str, Any]) -> None:
        """작업 실행 전 호출되는 hook
        
        서브클래스에서 선택적으로 구현 가능
        
        Args:
            context: 이전 작업들의 결과를 담은 컨텍스트
        """
        pass

    def _execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """실제 작업 실행 (서브클래스에서 구현)

        Args:
            context: 이전 작업들의 결과를 담은 컨텍스트

        Returns:
            작업 결과 딕셔너리
        """
        raise NotImplementedError

    def after_execute(self, context: Dict[str, Any], result: Dict[str, Any]) -> None:
        """작업 실행 후 호출되는 hook
        
        서브클래스에서 선택적으로 구현 가능
        
        Args:
            context: 이전 작업들의 결과를 담은 컨텍스트
            result: _execute에서 반환한 작업 결과
        """
        pass

    def on_error(self, context: Dict[str, Any], error: Exception) -> None:
        """작업 실행 중 에러 발생 시 호출되는 hook
        
        서브클래스에서 선택적으로 구현 가능
        
        Args:
            context: 이전 작업들의 결과를 담은 컨텍스트
            error: 발생한 예외
        """
        pass

    def _update_task_status(self, task_id: int, status: str) -> bool:
        """Task 상태를 업데이트합니다.
        
        Args:
            task_id: 업데이트할 task ID
            status: 변경할 상태 (e.g., 'processing', 'completed', 'failed')
            
        Returns:
            성공 여부
        """
        if task_id is None or self.job_queue_service is None:
            return False
        
        try:
            result = self.job_queue_service.update_task_status(task_id, status)
            if result:
                self.logger.info(f"Task status updated: task_id={task_id}, status={status}")
            else:
                self.logger.warning(f"Failed to update task status: task_id={task_id}, status={status}")
            return result
        except Exception as e:
            self.logger.error(f"Error updating task status: {str(e)}", exc_info=True)
            return False

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """작업 실행의 전체 흐름을 관리 (Template Method)

        Args:
            context: 이전 작업들의 결과를 담은 컨텍스트

        Returns:
            작업 결과 딕셔너리
        """
        try:
            # 0. 지연 시간 적용 (테스트/디버깅용)
            if self.delay_seconds is not None and self.delay_seconds > 0:
                self.logger.info(f"⏳ Delaying task execution for {self.delay_seconds} seconds...")
                time.sleep(self.delay_seconds)
                self.logger.info(f"✓ Delay completed")

            # 1. 실행 전 hook
            self.before_execute(context)
            
            # 2. 예외 발생 옵션 (테스트/디버깅용)
            if self.raise_exception is not None:
                self.logger.error(f"❌ Raising exception as configured: {self.raise_exception}")
                raise self.raise_exception
            
            # 3. 실제 작업 실행
            result = self._execute(context)
            
            # 4. 실행 후 hook
            self.after_execute(context, result)

            return result
            
        except Exception as e:
            # 5. 에러 발생 시 hook
            self.on_error(context, e)
            raise