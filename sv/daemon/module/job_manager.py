from typing import Optional, Dict, Any

from sv.utils.logger import setup_logger
from sv.backend.service.service_manager import get_service_manager
logger = setup_logger(__name__)


class JobManager:
    """Job 생명주기 관리"""
    
    def __init__(self):
        self.job_queue_service = get_service_manager().get_job_queue_service()
    
    def add_job(self, frfr_id: str, analysis_id: str) -> Optional[int]:
        """
        새로운 Job 추가 (Main Loop에서 호출)
        
        Args:
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            
        Returns:
            생성된 job_id 또는 None (중복 또는 에러)
        """
        try:
            job_id = self.job_queue_service.add_job(frfr_id, analysis_id)
            if job_id:
                logger.info(f"✅ Job added: job_id={job_id}, frfr_id={frfr_id}")
            else:
                logger.warning(f"⚠️ Job already exists: frfr_id={frfr_id}")
            return job_id
        except Exception as e:
            logger.error(f"❌ Error adding job: {str(e)}", exc_info=True)
            return None
    
    def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Job 상태 조회
        
        Args:
            job_id: 조회할 job ID
            
        Returns:
            Job 정보 또는 None
        """
        try:
            return self.job_queue_service.get_job_status(job_id)
        except Exception as e:
            logger.error(f"❌ Error getting job status: {str(e)}")
            return None
    
    def get_next_pending_job(self) -> Optional[Dict[str, Any]]:
        """
        다음 PENDING Job 가져오기
        
        Returns:
            {'job_id': int, 'frfr_id': str} 또는 None
        """
        try:
            job_info = self.job_queue_service.get_next_job()
            if job_info:
                logger.debug(f"Next pending job: {job_info}")
            return job_info
        except Exception as e:
            logger.error(f"❌ Error getting next job: {str(e)}")
            return None
    
    def update_job_status(self, job_id: int, status: str) -> bool:
        """
        Job 상태 업데이트
        
        Args:
            job_id: 업데이트할 job ID
            status: 새로운 상태
            
        Returns:
            성공 여부
        """
        try:
            self.job_queue_service.update_job_status(job_id, status)
            logger.info(f"✓ Job {job_id} status updated to {status}")
            return True
        except Exception as e:
            logger.error(f"❌ Error updating job status: {str(e)}")
            return False

