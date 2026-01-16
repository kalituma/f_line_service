"""
에러 발생 테스트용 Task

다양한 에러 시나리오를 테스트하기 위한 Task
"""
from typing import Dict, Any, Optional
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)


class ErrorTestTask(TaskBase):
    """에러 발생 테스트를 위한 Task"""
    
    def __init__(
        self,
        task_name: str = "ErrorTestTask",
        error_type: Optional[str] = None,
        error_message: str = "테스트 에러",
        fail_on_item_index: Optional[int] = None,
        fail_on_condition: Optional[callable] = None,
        delay_seconds: Optional[float] = None,
        raise_exception: Optional[Exception] = None
    ):
        """
        Args:
            task_name: Task 이름
            error_type: 발생시킬 에러 타입 
                       ('ValueError', 'RuntimeError', 'KeyError', 'TypeError', None)
                       None이면 에러 발생 안 함
            error_message: 에러 메시지
            fail_on_item_index: 특정 아이템 인덱스에서만 에러 발생 (None이면 항상)
            fail_on_condition: 조건 함수 (context를 받아서 bool 반환)
            delay_seconds: 작업 실행 시 지연 시간(초). None이면 지연 없음
            raise_exception: 작업 실행 시 발생시킬 예외. None이면 정상 실행
        """
        super().__init__(task_name, delay_seconds, raise_exception)
        self.error_type = error_type
        self.error_message = error_message
        self.fail_on_item_index = fail_on_item_index
        self.fail_on_condition = fail_on_condition
        self.execution_count = 0
        
    def _should_fail(self, context: Dict[str, Any]) -> bool:
        """에러를 발생시켜야 하는지 판단"""
        # 에러 타입이 None이면 실패하지 않음
        if self.error_type is None:
            return False
            
        # 특정 인덱스에서만 실패
        if self.fail_on_item_index is not None:
            current_index = context.get('item_index', 0)
            if current_index != self.fail_on_item_index:
                return False
                
        # 조건 함수가 있으면 확인
        if self.fail_on_condition is not None:
            if not self.fail_on_condition(context):
                return False
                
        return True
        
    def _execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """실제 작업 실행"""
        self.execution_count += 1
        item_index = context.get('item_index', 0)
        
        logger.info(f"▶️  {self.task_name} 실행 (#{self.execution_count}, item_index={item_index})")
        
        # 에러 발생 여부 확인
        if self._should_fail(context):
            error_classes = {
                'ValueError': ValueError,
                'RuntimeError': RuntimeError,
                'KeyError': KeyError,
                'TypeError': TypeError,
                'Exception': Exception
            }
            
            error_class = error_classes.get(self.error_type, RuntimeError)
            full_message = f"{self.error_message} (실행 #{self.execution_count}, item_index={item_index})"
            
            logger.error(f"❌ 의도적 에러 발생: {self.error_type} - {full_message}")
            raise error_class(full_message)
        
        # 정상 실행
        logger.info(f"✅ {self.task_name} 정상 완료 (#{self.execution_count})")
        return {
            "status": "success",
            "execution_count": self.execution_count,
            "message": f"{self.task_name} 성공"
        }

