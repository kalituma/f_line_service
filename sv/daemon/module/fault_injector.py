"""
Fault Injection 모듈

테스트나 디버깅 시 특정 메서드에서 선택적으로 exception을 발생시킬 수 있는 기능 제공
"""

from typing import Dict, Callable, Optional, Any
import random


class FaultInjector:
    """
    선택적으로 exception을 발생시키는 Fault Injector
    
    Example:
        ```python
        # 1. 특정 메서드에서 항상 에러 발생
        injector = FaultInjector()
        injector.register_fault(
            method_name="_create_work_directory",
            exception=RuntimeError("디렉토리 생성 실패 시뮬레이션"),
            probability=1.0  # 100% 발생
        )
        
        # 2. 확률적 에러 발생
        injector.register_fault(
            method_name="_execute_primary_task",
            exception=TimeoutError("타임아웃 시뮬레이션"),
            probability=0.3  # 30% 확률
        )
        
        # 3. 조건부 에러 발생
        injector.register_fault(
            method_name="_split_data",
            exception=ValueError("데이터 분할 실패"),
            condition=lambda ctx: ctx.get('data_count', 0) > 100  # 100개 이상일 때만
        )
        
        # 4. Executor에 주입
        executor = FlineTaskSplitExecutor.remote(
            executor_id=1,
            base_work_dir="/path/to/work",
            update_url="http://example.com",
            fault_injector=injector  # Optional로 주입
        )
        ```
    """
    
    def __init__(self, enabled: bool = True):
        """
        Args:
            enabled: Fault injection 활성화 여부 (False면 모든 fault 무시)
        """
        self.enabled = enabled
        self._faults: Dict[str, Dict[str, Any]] = {}
    
    def register_fault(
        self,
        method_name: str,
        exception: Exception,
        probability: float = 1.0,
        condition: Optional[Callable[[Dict[str, Any]], bool]] = None,
        max_triggers: Optional[int] = None
    ) -> None:
        """
        특정 메서드에 fault를 등록합니다.
        
        Args:
            method_name: 대상 메서드 이름 (예: "_create_work_directory")
            exception: 발생시킬 exception 객체
            probability: 발생 확률 (0.0 ~ 1.0)
            condition: 조건 함수 (컨텍스트를 받아 bool 반환)
            max_triggers: 최대 트리거 횟수 (None이면 무제한)
        """
        self._faults[method_name] = {
            'exception': exception,
            'probability': probability,
            'condition': condition,
            'max_triggers': max_triggers,
            'trigger_count': 0
        }
    
    def unregister_fault(self, method_name: str) -> None:
        """등록된 fault를 제거합니다."""
        if method_name in self._faults:
            del self._faults[method_name]
    
    def clear_all(self) -> None:
        """모든 fault를 제거합니다."""
        self._faults.clear()
    
    def inject(self, method_name: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        메서드 시작 시 호출하여 fault를 주입합니다.
        
        Args:
            method_name: 현재 메서드 이름
            context: 메서드 실행 컨텍스트 (조건 판단에 사용)
            
        Raises:
            등록된 exception (조건이 만족되면)
        """
        if not self.enabled:
            return
        
        if method_name not in self._faults:
            return
        
        fault = self._faults[method_name]
        
        # 최대 트리거 횟수 체크
        if fault['max_triggers'] is not None:
            if fault['trigger_count'] >= fault['max_triggers']:
                return
        
        # 조건 체크
        if fault['condition'] is not None:
            ctx = context or {}
            if not fault['condition'](ctx):
                return
        
        # 확률 체크
        if random.random() > fault['probability']:
            return
        
        # Fault 발생
        fault['trigger_count'] += 1
        raise fault['exception']
    
    def reset_counters(self) -> None:
        """모든 fault의 트리거 카운터를 리셋합니다."""
        for fault in self._faults.values():
            fault['trigger_count'] = 0
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Fault injection 통계를 반환합니다.
        
        Returns:
            각 메서드별 트리거 횟수 및 설정 정보
        """
        return {
            method_name: {
                'trigger_count': fault['trigger_count'],
                'max_triggers': fault['max_triggers'],
                'probability': fault['probability'],
                'has_condition': fault['condition'] is not None
            }
            for method_name, fault in self._faults.items()
        }


class FaultInjectorBuilder:
    """
    Fluent API를 제공하는 FaultInjector 빌더
    
    Example:
        ```python
        injector = (
            FaultInjectorBuilder()
            .with_fault("_create_work_directory", RuntimeError("디렉토리 생성 실패"))
            .with_probability(0.5)
            .with_fault("_split_data", ValueError("분할 실패"), probability=0.3)
            .build()
        )
        ```
    """
    
    def __init__(self):
        self._injector = FaultInjector()
        self._current_method: Optional[str] = None
        self._current_exception: Optional[Exception] = None
        self._current_probability: float = 1.0
        self._current_condition: Optional[Callable] = None
        self._current_max_triggers: Optional[int] = None
    
    def with_fault(
        self,
        method_name: str,
        exception: Exception,
        probability: float = 1.0,
        condition: Optional[Callable] = None,
        max_triggers: Optional[int] = None
    ) -> 'FaultInjectorBuilder':
        """
        Fault를 추가합니다.
        
        Args:
            method_name: 대상 메서드 이름
            exception: 발생시킬 exception
            probability: 발생 확률 (0.0 ~ 1.0)
            condition: 조건 함수
            max_triggers: 최대 트리거 횟수
        """
        # 이전 fault가 있으면 등록
        self._register_current_fault()
        
        # 새 fault 설정
        self._current_method = method_name
        self._current_exception = exception
        self._current_probability = probability
        self._current_condition = condition
        self._current_max_triggers = max_triggers
        
        return self
    
    def with_probability(self, probability: float) -> 'FaultInjectorBuilder':
        """발생 확률을 설정합니다 (0.0 ~ 1.0)"""
        self._current_probability = probability
        return self
    
    def with_condition(self, condition: Callable[[Dict[str, Any]], bool]) -> 'FaultInjectorBuilder':
        """조건 함수를 설정합니다"""
        self._current_condition = condition
        return self
    
    def with_max_triggers(self, max_triggers: int) -> 'FaultInjectorBuilder':
        """최대 트리거 횟수를 설정합니다"""
        self._current_max_triggers = max_triggers
        return self
    
    def enabled(self, enabled: bool) -> 'FaultInjectorBuilder':
        """Fault injection 활성화 여부를 설정합니다"""
        self._injector.enabled = enabled
        return self
    
    def build(self) -> FaultInjector:
        """FaultInjector를 생성합니다"""
        # 마지막 fault 등록
        self._register_current_fault()
        return self._injector
    
    def _register_current_fault(self) -> None:
        """현재 설정된 fault를 등록합니다"""
        if self._current_method and self._current_exception:
            self._injector.register_fault(
                method_name=self._current_method,
                exception=self._current_exception,
                probability=self._current_probability,
                condition=self._current_condition,
                max_triggers=self._current_max_triggers
            )
            
            # 리셋
            self._current_method = None
            self._current_exception = None
            self._current_probability = 1.0
            self._current_condition = None
            self._current_max_triggers = None


# 편의 함수들
def create_simple_injector(method_faults: Dict[str, Exception]) -> FaultInjector:
    """
    간단한 fault injector를 생성합니다.
    
    Args:
        method_faults: {메서드명: Exception} 딕셔너리
        
    Example:
        ```python
        injector = create_simple_injector({
            "_create_work_directory": RuntimeError("디렉토리 생성 실패"),
            "_split_data": ValueError("데이터 분할 실패")
        })
        ```
    """
    injector = FaultInjector()
    for method_name, exception in method_faults.items():
        injector.register_fault(method_name, exception)
    return injector


def create_chaos_injector(
    methods: list[str],
    exceptions: list[Exception],
    probability: float = 0.1
) -> FaultInjector:
    """
    Chaos Engineering용 injector를 생성합니다 (랜덤하게 에러 발생).
    
    Args:
        methods: 대상 메서드 리스트
        exceptions: 발생시킬 exception 리스트 (랜덤 선택)
        probability: 각 메서드의 에러 발생 확률
        
    Example:
        ```python
        injector = create_chaos_injector(
            methods=["_create_work_directory", "_split_data", "_execute_primary_task"],
            exceptions=[RuntimeError("랜덤 에러"), TimeoutError("타임아웃")],
            probability=0.1  # 각 메서드에서 10% 확률로 에러 발생
        )
        ```
    """
    injector = FaultInjector()
    for method in methods:
        exception = random.choice(exceptions)
        injector.register_fault(method, exception, probability=probability)
    return injector

