import asyncio
from typing import Optional
from enum import Enum

from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class AppState(Enum):
    """애플리케이션 상태"""
    INITIALIZING = "initializing"      # 초기화 중
    READY = "ready"                    # 준비 완료
    SHUTDOWN = "shutdown"              # 종료

class AppStateManager:
    """애플리케이션 상태 관리자"""
    
    def __init__(self):
        """상태 관리자 초기화"""
        self._state = AppState.INITIALIZING
        self._lock = asyncio.Lock()
        self._state_changed_event = asyncio.Event()
    
    async def wait_ready(self) -> None:
        """
        애플리케이션이 READY 상태가 될 때까지 대기합니다.
        """
        while self._state != AppState.READY:
            self._state_changed_event.clear()
            await self._state_changed_event.wait()
    
    async def set_state(self, state: AppState) -> None:
        """
        애플리케이션 상태를 변경합니다.
        
        Args:
            state: 변경할 상태
        """

        async with self._lock:
            old_state = self._state
            self._state = state
            logger.info(f"App state changed: {old_state.value} → {state.value}")
            self._state_changed_event.set()
    
    def get_state(self) -> AppState:
        """
        현재 애플리케이션 상태를 반환합니다.
        
        Returns:
            AppState: 현재 상태
        """
        return self._state
    
    def is_ready(self) -> bool:
        """
        애플리케이션이 준비되었는지 확인합니다.
        
        Returns:
            bool: 준비 완료 여부
        """
        return self._state == AppState.READY


# 전역 상태 관리자 인스턴스
_app_state_manager: Optional[AppStateManager] = None


def get_app_state_manager() -> AppStateManager:
    """
    전역 앱 상태 관리자를 반환합니다.
    
    Returns:
        AppStateManager: 상태 관리자 인스턴스
    """
    global _app_state_manager
    if _app_state_manager is None:
        _app_state_manager = AppStateManager()
    return _app_state_manager

