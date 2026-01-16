from typing import Optional, Dict, Any

from sv.utils.logger import setup_logger
from sv.backend.service.service_manager import get_service_manager
logger = setup_logger(__name__)


class WorkManager:
    """Job 생명주기 관리"""
    
    def __init__(self):
        self._work_queue_service = None
    
    @property
    def work_queue_service(self):        
        if self._work_queue_service is None:
            self._work_queue_service = get_service_manager().get_work_queue_service()
        return self._work_queue_service
    
    def add_work(self, frfr_id: str, analysis_id: str) -> Optional[int]:
        """
        새로운 Work 추가 (Main Loop에서 호출)
        
        Args:
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            
        Returns:
            생성된 work_id 또는 None (중복 또는 에러)
        """
        try:
            work_id = self.work_queue_service.add_work(frfr_id, analysis_id)
            if work_id:
                logger.info(f"✅ Work added: work_id={work_id}, frfr_id={frfr_id}")
            else:
                logger.warning(f"⚠️ Work already exists: frfr_id={frfr_id}")
            return work_id
        except Exception as e:
            logger.error(f"❌ Error adding work: {str(e)}", exc_info=True)
            return None

    def get_work_status(self, work_id: int) -> Optional[Dict[str, Any]]:
        """
        Work 상태 조회

        Args:
            work_id: 조회할 work ID

        Returns:
            Work 정보 또는 None
        """
        try:
            return self.work_queue_service.get_work_status(work_id)
        except Exception as e:
            logger.error(f"❌ Error getting work status: {str(e)}")
            return None

    def get_next_pending_work(self) -> Optional[Dict[str, Any]]:
        """
        다음 PENDING Work 가져오기
        
        Returns:
            {'work_id': int, 'frfr_id': str} 또는 None
        """
        try:
            work_info = self.work_queue_service.get_next_work()
            if work_info:
                logger.debug(f"Next pending work: {work_info}")
            return work_info
        except Exception as e:
            logger.error(f"❌ Error getting next work: {str(e)}")
            return None
    
    def update_work_status(self, work_id: int, status: str) -> bool:
        """
        Work 상태 업데이트
        
        Args:
            work_id: 업데이트할 work ID
            status: 새로운 상태
            
        Returns:
            성공 여부
        """
        try:
            self.work_queue_service.update_work_status(work_id, status)
            logger.info(f"✓ Work {work_id} status updated to {status}")
            return True
        except Exception as e:
            logger.error(f"❌ Error updating work status: {str(e)}")
            return False

