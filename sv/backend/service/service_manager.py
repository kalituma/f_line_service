"""
애플리케이션의 모든 서비스를 싱글톤으로 관리하는 매니저
"""
from typing import Optional, Dict, Any

from sv.backend.service.job_queue_service import JobQueueService
from sv.backend.service.work_queue_service import WorkQueueService
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)


class ServiceManager:
    """
    애플리케이션의 모든 서비스를 싱글톤으로 관리하는 매니저.
    startup 시점에 모든 서비스를 초기화하여 등록하고,
    이후 endpoint에서는 이미 생성된 서비스를 재사용한다.
    """
    _instance: Optional['ServiceManager'] = None
    _services: Dict[str, Any] = {}
    _initialized: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize_all_services(self) -> bool:
        """
        모든 서비스를 초기화하고 등록한다.
        
        Returns:
            bool: 초기화 성공 여부
        """
        if self._initialized:
            logger.warning("ServiceManager already initialized")
            return True
        
        try:
            logger.info("Initializing all services...")
            
            # JobQueueService 초기화
            job_queue_service = JobQueueService()
            self._services['job_queue'] = job_queue_service
            logger.info("✓ JobQueueService initialized and registered")
            
            work_queue_service = WorkQueueService()
            self._services['work_queue'] = work_queue_service
            logger.info("✓ WorkQueueService initialized and registered")
            
            self._initialized = True
            logger.info(f"✅ All services initialized successfully (total: {len(self._services)})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize services: {str(e)}")
            return False
    
    def get_job_queue_service(self) -> JobQueueService:
        """
        JobQueueService 인스턴스를 반환한다.
        
        Returns:
            JobQueueService: 등록된 JobQueueService 인스턴스
        
        Raises:
            RuntimeError: 서비스가 초기화되지 않은 경우
        """
        if not self._initialized:
            raise RuntimeError("ServiceManager not initialized. Call initialize_all_services() first.")
        
        return self._services.get('job_queue')

    def get_work_queue_service(self) -> WorkQueueService:
        """
        JobQueueService 인스턴스를 반환한다.

        Returns:
            JobQueueService: 등록된 JobQueueService 인스턴스

        Raises:
            RuntimeError: 서비스가 초기화되지 않은 경우
        """
        if not self._initialized:
            raise RuntimeError("ServiceManager not initialized. Call initialize_all_services() first.")

        return self._services.get('work_queue')
    
    def is_initialized(self) -> bool:
        """
        서비스 매니저가 초기화되었는지 확인한다.
        
        Returns:
            bool: 초기화 여부
        """
        return self._initialized
    
    def reset(self) -> None:
        """
        테스트용: 서비스 매니저를 초기화 이전 상태로 리셋한다.
        """
        self._services.clear()
        self._initialized = False
        logger.info("ServiceManager reset")


# 편의 함수
def get_service_manager() -> ServiceManager:
    """
    ServiceManager 싱글톤 인스턴스를 반환한다.
    
    Returns:
        ServiceManager: 싱글톤 인스턴스
    """
    return ServiceManager()
