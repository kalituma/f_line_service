import uvicorn
import time
import logging as logging_module
import asyncio
from threading import Thread, Event as ThreadEvent
from typing import Callable, Optional, Any

from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class ThreadManager:
    """Listener, Web Server, Ray Job Monitor 스레드 중앙 관리"""
    
    def __init__(
        self,
        poll_interval: float = 2.0,
        web_app: Optional[Any] = None,
        web_host: str = "localhost",
        web_port: int = 8090,
        execution_engine: Optional[Any] = None
    ):
        """
        Args:
            poll_interval: Event Listener 폴링 간격
            web_app: FastAPI 또는 Starlette 웹 앱 인스턴스
            web_host: 웹 서버 호스트
            web_port: 웹 서버 포트
            execution_engine: ExecutionEngine 인스턴스 (Ray Job Monitor용)
        """
        self.poll_interval = poll_interval
        self.web_app = web_app
        self.web_host = web_host
        self.web_port = web_port
        self.execution_engine = execution_engine
        
        self.running = False
        self.stop_event = ThreadEvent()
        self.listener_thread: Optional[Thread] = None
        self.web_thread: Optional[Thread] = None
        self.monitor_thread: Optional[Thread] = None
        
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
        logger.info("🎧 DB Change Listener started")
        
        while not self.stop_event.is_set():
            try:
                if self.on_check_changes:
                    self.on_check_changes()
                
                time.sleep(self.poll_interval)
            
            except Exception as e:
                logger.error(f"❌ Error in listener thread: {str(e)}", exc_info=True)
                time.sleep(self.poll_interval)
        
        logger.info("🎧 DB Change Listener stopped")

    def _web_server_thread_func(self) -> None:
        """Web 서버 스레드"""
        logger.info("🌐 Web Server started")
        
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
    
    def _ray_monitor_thread_func(self) -> None:
        """Ray Job 모니터 스레드 (ExecutionEngine의 pending jobs 모니터링)"""
        logger.info("⚡ Ray Job Monitor started")
        
        while not self.stop_event.is_set():
            try:
                if not self.execution_engine:
                    time.sleep(self.poll_interval)
                    continue
                
                # ExecutionEngine에서 완료된 작업 확인 및 처리
                completed_jobs = self.execution_engine.check_and_process_completed_jobs(timeout=1.0)
                
                if completed_jobs:
                    logger.debug(f"Processed {len(completed_jobs)} completed jobs")
                
                # pending jobs가 없으면 짧은 대기
                pending_snapshot = self.execution_engine.get_pending_jobs_snapshot()
                if not pending_snapshot:
                    time.sleep(self.poll_interval)
                else:
                    # pending jobs가 있으면 더 자주 체크 (1초)
                    time.sleep(1.0)
            
            except Exception as e:
                logger.error(f"❌ Error in Ray monitor thread: {str(e)}", exc_info=True)
                time.sleep(self.poll_interval)
        
        logger.info("⚡ Ray Job Monitor stopped")
    
    def set_execution_engine(self, execution_engine: Any) -> None:
        """
        ExecutionEngine 설정 (나중에 설정 가능)
        
        Args:
            execution_engine: ExecutionEngine 인스턴스
        """
        self.execution_engine = execution_engine
        logger.info("✓ ExecutionEngine set for Ray Job Monitor")
    
    def start(self) -> None:
        """모든 스레드 시작"""
        if self.running:
            logger.warning("⚠️ ThreadManager is already running")
            return
        
        self.running = True
        self.stop_event.clear()
        
        # 1. Event Listener 시작
        self.listener_thread = Thread(
            target=self._listener_thread_func,
            daemon=True,
            name="DBChangeListener"
        )
        self.listener_thread.start()
        logger.info("✓ Event Listener thread started")

        # 2. Web Server 시작
        if self.web_app:
            self.web_thread = Thread(
                target=self._web_server_thread_func,
                daemon=True,
                name="FlineWebServer"
            )
            self.web_thread.start()
            logger.info("✓ Web Server thread started")
        
        # 3. Ray Job Monitor 시작
        if self.execution_engine:
            self.monitor_thread = Thread(
                target=self._ray_monitor_thread_func,
                daemon=True,
                name="RayJobMonitor"
            )
            self.monitor_thread.start()
            logger.info("✓ Ray Job Monitor thread started")
        else:
            logger.warning("⚠️ ExecutionEngine not set, Ray Job Monitor not started")
    
    def stop(self) -> None:
        """모든 스레드 중지"""
        if not self.running:
            return
        
        logger.info("Stopping all threads...")
        self.running = False
        self.stop_event.set()
        
        # 스레드 종료 대기
        threads_to_stop = [
            ("Event Listener", self.listener_thread),
            ("Web Server", self.web_thread),
            ("Ray Job Monitor", self.monitor_thread)
        ]
        
        for thread_name, thread in threads_to_stop:
            if thread and thread.is_alive():
                logger.info(f"Stopping {thread_name} thread...")
                thread.join(timeout=5)
                if thread.is_alive():
                    logger.warning(f"⚠️ {thread_name} thread did not stop gracefully")
                else:
                    logger.info(f"✓ {thread_name} thread stopped")
        
        logger.info("✓ All threads stopped")
    
    def get_status(self) -> dict:
        """모든 스레드 상태 반환"""
        return {
            'running': self.running,
            'listener_active': self.listener_thread and self.listener_thread.is_alive(),
            'web_server_active': self.web_app and self.web_thread and self.web_thread.is_alive(),
            'ray_monitor_active': self.monitor_thread and self.monitor_thread.is_alive()
        }

