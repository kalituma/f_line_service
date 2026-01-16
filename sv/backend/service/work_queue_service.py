import os
from typing import Optional, List, Dict, Any

from sv import DEFAULT_JOB_QUEUE_DB
from sv.backend.db.work_queue_db import WorkQueue
from sv.backend.work_status import WorkStatus
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class WorkQueueService:
    """
    WorkQueue를 활용한 작업 큐 관리 서비스
    """
    
    def __init__(self, db_path: str = DEFAULT_JOB_QUEUE_DB):
        """
        서비스 초기화
        
        Args:
            db_path: WorkQueue 데이터베이스 경로 (기본값: DEFAULT_JOB_QUEUE_DB from sv/__init__.py)
        
        Note:
            같은 db_path로 생성된 WorkQueue는
            DBConnectionManager를 통해 DB 연결을 공유합니다.
        """
        self.work_queue = WorkQueue(db_path)
        
        # 테이블 초기화
        self.work_queue._init_db()
        
        logger.info(f"WorkQueueService initialized with db_path={db_path}")
    
    def add_work(self, frfr_id: str, analysis_id: str, status: 'WorkStatus' = None) -> Optional[int]:
        """
        새 작업을 큐에 추가합니다.
        중복된 frfr_id와 analysis_id 조합은 추가되지 않습니다.
        
        Args:
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            status: 초기 작업 상태 (기본값: PENDING)
            
        Returns:
            작업 ID 또는 None (중복된 경우)
        """
        from sv.backend.work_status import WorkStatus
        if status is None:
            status = WorkStatus.PENDING
            
        try:
            work_id = self.work_queue.add_work(frfr_id, analysis_id, status)
            if work_id:
                logger.info(f"Work added successfully: work_id={work_id}, frfr_id={frfr_id}, analysis_id={analysis_id}")
            else:
                logger.warning(f"Failed to add work (possibly duplicate): frfr_id={frfr_id}, analysis_id={analysis_id}")
            return work_id
        except Exception as e:
            logger.error(f"Error adding work: {str(e)}")
            raise
    
    def get_next_work(self) -> Optional[Dict[str, Any]]:
        """
        FIFO 순서로 다음 작업을 가져옵니다.
        
        Returns:
            {'work_id': int, 'frfr_id': str, 'analysis_id': str} 또는 None (작업이 없는 경우)
        """
        try:
            work_info = self.work_queue.get_next_work()
            if work_info:
                logger.info(f"Next work retrieved: work_id={work_info['work_id']}, frfr_id={work_info['frfr_id']}")
            else:
                logger.info("No works available")
            return work_info
        except Exception as e:
            logger.error(f"Error getting next work: {str(e)}")
            raise
    
    def get_all_works(self) -> List[Dict[str, Any]]:
        """
        모든 작업 정보를 조회합니다.
        
        Returns:
            모든 작업 정보 리스트
        """
        try:
            works = self.work_queue.get_all_works()
            logger.info(f"Retrieved all works: count={len(works)}")
            return works
        except Exception as e:
            logger.error(f"Error getting all works: {str(e)}")
            raise
    
    def get_work_by_id(self, work_id: int) -> Optional[Dict[str, Any]]:
        """
        작업 ID로 작업 정보를 조회합니다.
        
        Args:
            work_id: 작업 ID
            
        Returns:
            작업 정보 딕셔너리 또는 None (작업이 없는 경우)
        """
        try:
            result = self.work_queue.get_work_by_id(work_id)
            if result:
                logger.info(f"Work retrieved: work_id={work_id}")
            else:
                logger.warning(f"Work not found: work_id={work_id}")
            return result
        except Exception as e:
            logger.error(f"Error getting work by id: {str(e)}")
            raise
    
    def get_works_by_frfr_id(self, frfr_id: str) -> List[Dict[str, Any]]:
        """
        산불 정보 ID로 작업들을 조회합니다.
        
        Args:
            frfr_id: 산불 정보 ID
            
        Returns:
            작업 정보 딕셔너리 리스트
        """
        try:
            result = self.work_queue.get_works_by_frfr_id(frfr_id)
            logger.info(f"Works retrieved by frfr_id={frfr_id}, count={len(result)}")
            return result
        except Exception as e:
            logger.error(f"Error getting works by frfr_id: {str(e)}")
            raise
    
    def get_works_by_analysis_id(self, analysis_id: str) -> List[Dict[str, Any]]:
        """
        분석 ID로 작업들을 조회합니다.
        
        Args:
            analysis_id: 분석 ID
            
        Returns:
            작업 정보 딕셔너리 리스트
        """
        try:
            result = self.work_queue.get_works_by_analysis_id(analysis_id)
            logger.info(f"Works retrieved by analysis_id={analysis_id}, count={len(result)}")
            return result
        except Exception as e:
            logger.error(f"Error getting works by analysis_id: {str(e)}")
            raise
    
    def get_works_by_status(self, status: 'WorkStatus') -> List[Dict[str, Any]]:
        """
        특정 상태의 모든 작업들을 조회합니다.
        
        Args:
            status: 조회할 작업 상태 (JobStatus enum)
            
        Returns:
            작업 정보 딕셔너리 리스트
        """
        from sv.backend.work_status import WorkStatus
        try:
            status_value = status.value if isinstance(status, WorkStatus) else status
            result = self.work_queue.get_works_by_status(status_value)
            logger.debug(f"Works retrieved with status={status_value}, count={len(result)}")
            return result
        except Exception as e:
            logger.error(f"Error getting works by status: {str(e)}")
            raise

    def has_pending_works(self) -> bool:
        """
        Pending 상태의 작업이 존재하는지 확인합니다.

        Returns:
            Pending 작업 존재 여부

        """
        try:
            pending_works = self.get_works_by_status(WorkStatus.PENDING)
            result = len(pending_works) > 0
            logger.info(f"Pending works check: has_pending={result}, count={len(pending_works)}")
            return result
        except Exception as e:
            logger.error(f"Error checking pending works: {str(e)}")
            raise
    
    def update_work_status(self, work_id: int, status: 'WorkStatus') -> bool:
        """
        작업의 상태를 업데이트합니다.
        
        Args:
            work_id: 작업 ID
            status: 변경할 작업 상태 (JobStatus enum)
            
        Returns:
            성공 여부
        """
        from sv.backend.work_status import WorkStatus
        try:
            status_value = status.value if isinstance(status, WorkStatus) else status
            result = self.work_queue.update_work_status(work_id, status_value)
            return result
        except Exception as e:
            logger.error(f"Error updating work status: {str(e)}")
            raise
    
    def get_work_status(self, work_id: int) -> Optional[Dict[str, Any]]:
        """
        특정 작업의 상태를 조회합니다.
        
        Args:
            work_id: 작업 ID
            
        Returns:
            작업의 상태 정보 또는 None
        """
        try:
            result = self.work_queue.get_work_status(work_id)
            if result:
                logger.info(f"Work status retrieved: work_id={work_id}, status={result['status']}")
            else:
                logger.warning(f"Work not found: work_id={work_id}")
            return result
        except Exception as e:
            logger.error(f"Error getting work status: {str(e)}")
            raise
    
    def count_works_by_status(self, status: 'WorkStatus') -> int:
        """
        특정 상태의 작업 개수를 조회합니다.
        
        Args:
            status: 조회할 작업 상태 (JobStatus enum)
            
        Returns:
            해당 상태의 작업 개수

        """
        from sv.backend.work_status import WorkStatus
        try:
            status_value = status.value if isinstance(status, WorkStatus) else status
            count = self.work_queue.count_works_by_status(status_value)
            logger.info(f"Work count by status: status={status_value}, count={count}")
            return count
        except Exception as e:
            logger.error(f"Error counting works by status: {str(e)}")
            raise
    
    def delete_work(self, work_id: int) -> bool:
        """
        작업을 삭제합니다.
        
        Args:
            work_id: 작업 ID
            
        Returns:
            성공 여부
        """
        try:
            success = self.work_queue.delete_work(work_id)
            if success:
                logger.info(f"Work deleted: work_id={work_id}")
            else:
                logger.warning(f"Failed to delete work: work_id={work_id}")
            return success
        except Exception as e:
            logger.error(f"Error deleting work: {str(e)}")
            raise
    
    def count_all_works(self) -> int:
        """
        전체 작업 개수를 조회합니다.
        
        Returns:
            전체 작업 개수
        """
        try:
            count = self.work_queue.count_all_works()
            logger.info(f"Total works count: {count}")
            return count
        except Exception as e:
            logger.error(f"Error counting all works: {str(e)}")
            raise
    
    def count_works_by_frfr_id(self, frfr_id: str) -> int:
        """
        산불 정보 ID별 작업 개수를 조회합니다.
        
        Args:
            frfr_id: 산불 정보 ID
            
        Returns:
            해당 frfr_id의 작업 개수
        """
        try:
            count = self.work_queue.count_works_by_frfr_id(frfr_id)
            logger.info(f"Works count by frfr_id={frfr_id}: {count}")
            return count
        except Exception as e:
            logger.error(f"Error counting works by frfr_id: {str(e)}")
            raise
