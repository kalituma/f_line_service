"""
Thread 관리 담당 클래스
Listener Thread, Fallback Polling Thread, Web Server Thread 관리
"""

import time
import logging as logging_module
import asyncio
from threading import Thread, Event as ThreadEvent
from typing import Callable, Optional, Any

from sv.utils.logger import setup_logger

logger = setup_logger(__name__)
logging = logging_module


class ThreadManager:
    """Listener 및 Polling 스레드 관리"""
    
    def __init__(
        self,
        poll_interval: float = 2.0,
        fallback_poll_interval: int = 30,
        enable_event_listener: bool = True,
        enable_fallback_polling: bool = True,
        web_app: Optional[Any] = None,
        web_host: str = "localhost",
        web_port: int = 8090
    ):
        """
        Args:
            poll_interval: Event Listener 폴링 간격
            fallback_poll_interval: Fallback Polling 간격
            enable_event_listener: Event Listener 활성화 여부
            enable_fallback_polling: Fallback Polling 활성화 여부
            web_app: FastAPI 또는 Starlette 웹 앱 인스턴스
            web_host: 웹 서버 호스트
            web_port: 웹 서버 포트
        """
        self.poll_interval = poll_interval
        self.fallback_poll_interval = fallback_poll_interval
        self.enable_event_listener = enable_event_listener
        self.enable_fallback_polling = enable_fallback_polling
        self.web_app = web_app
        self.web_host = web_host
        self.web_port = web_port
        
        self.running = False
        self.stop_event = ThreadEvent()
        self.listener_thread: Optional[Thread] = None
        self.fallback_poll_thread: Optional[Thread] = None
        self.web_thread: Optional[Thread] = None
        
        # 콜백
        self.on_check_changes: Optional[Callable] = None
        self.on_process_jobs: Optional[Callable] = None
    
    def set_check_changes_callback(self, callback: Callable) -> None:
        """
        변경사항 체크 콜백 설정 (Listener에서 호출)
        
        Args:
            callback: 변경사항 확인 함수
        """
        self.on_check_changes = callback
    
    def set_process_jobs_callback(self, callback: Callable) -> None:
        """
        Job 처리 콜백 설정 (Polling에서 호출)
        
        Args:
            callback: Job 처리 함수
        """
        self.on_process_jobs = callback
    
    def _listener_thread_func(self) -> None:
        """DB 변경 감지 리스너 스레드"""
        logging.info("🎧 DB Change Listener started")
        
        while not self.stop_event.is_set():
            try:
                if self.on_check_changes:
                    self.on_check_changes()
                
                time.sleep(self.poll_interval)
            
            except Exception as e:
                logger.error(f"❌ Error in listener thread: {str(e)}", exc_info=True)
                time.sleep(self.poll_interval)
        
        logger.info("🎧 DB Change Listener stopped")
    
    def _fallback_poll_thread_func(self) -> None:
        """폴백 주기적 폴링 스레드"""
        logging.info("📊 Fallback Polling started")
        
        while not self.stop_event.is_set():
            try:
                logger.debug("Running fallback periodic polling")
                
                if self.on_process_jobs:
                    asyncio.run(self.on_process_jobs())
                
                time.sleep(self.fallback_poll_interval)
            
            except Exception as e:
                logger.error(f"❌ Error in fallback polling: {str(e)}", exc_info=True)
                time.sleep(self.fallback_poll_interval)
        
        logger.info("📊 Fallback Polling stopped")
    
    def _web_server_thread_func(self) -> None:
        """Web 서버 스레드"""
        try:
            import uvicorn
        except ImportError:
            logger.error("❌ uvicorn is not installed")
            return
        
        logging.info("🌐 Web Server started")
        
        try:
            uvicorn.run(
                self.web_app,
                host=self.web_host,
                port=self.web_port,
                log_level="info"
            )
        except Exception as e:
            logger.error(f"❌ Error in web server: {str(e)}", exc_info=True)
        
        logger.info("🌐 Web Server stopped")
    
    def start(self) -> None:
        """스레드 시작"""
        if self.running:
            logger.warning("⚠️ ThreadManager is already running")
            return
        
        self.running = True
        self.stop_event.clear()
        
        # Event Listener 시작
        if self.enable_event_listener:
            self.listener_thread = Thread(
                target=self._listener_thread_func,
                daemon=True,
                name="DBChangeListener"
            )
            self.listener_thread.start()
            logger.info("✓ Event Listener thread started")
        
        # Fallback Polling 시작
        if self.enable_fallback_polling:
            self.fallback_poll_thread = Thread(
                target=self._fallback_poll_thread_func,
                daemon=True,
                name="FallbackPoller"
            )
            self.fallback_poll_thread.start()
            logger.info("✓ Fallback Polling thread started")
        
        # Web Server 시작
        if self.web_app:
            self.web_thread = Thread(
                target=self._web_server_thread_func,
                daemon=True,
                name="FlineServer"
            )
            self.web_thread.start()
            logger.info("✓ Web Server thread started")
    
    def stop(self) -> None:
        """스레드 중지"""
        if not self.running:
            return
        
        logger.info("Stopping threads...")
        self.running = False
        self.stop_event.set()
        
        # 스레드 종료 대기
        if self.listener_thread:
            self.listener_thread.join(timeout=5)
        
        if self.fallback_poll_thread:
            self.fallback_poll_thread.join(timeout=5)
        
        if self.web_thread:
            self.web_thread.join(timeout=5)
        
        logger.info("✓ All threads stopped")
    
    def get_status(self) -> dict:
        """스레드 상태 반환"""
        return {
            'running': self.running,
            'listener_active': self.enable_event_listener and self.listener_thread and self.listener_thread.is_alive(),
            'polling_active': self.enable_fallback_polling and self.fallback_poll_thread and self.fallback_poll_thread.is_alive(),
            'web_server_active': self.web_app and self.web_thread and self.web_thread.is_alive()
        }

