import os
from typing import Optional, List, Dict, Any, TYPE_CHECKING

from sv import DEFAULT_JOB_QUEUE_DB
from sv.backend.work_status import WorkStatus
from sv.backend.db.job_queue_db import JobQueue
from sv.utils.logger import setup_logger

if TYPE_CHECKING:
    from sv.task.task_base import TaskBase

logger = setup_logger(__name__)

class JobQueueService:
    """
    JobQueue와 TaskQueue를 활용한 작업 큐 관리 서비스
    """
    
    def __init__(self, db_path: str = DEFAULT_JOB_QUEUE_DB):
        """
        서비스 초기화
        
        Args:
            db_path: JobQueue 데이터베이스 경로 (기본값: DEFAULT_JOB_QUEUE_DB from sv/__init__.py)
        
        Note:
            같은 db_path로 생성된 JobQueue와 TaskQueue는
            DBConnectionManager를 통해 DB 연결을 공유합니다.
        """
        self.job_queue = JobQueue(db_path)
        
        # 테이블 초기화
        self.job_queue._init_db()
        
        logger.info(f"JobQueueService initialized with db_path={db_path}")
    
    def add_job(self, work_id: int, frfr_id: str, analysis_id: str, video_url: str, 
                workspace: Optional[str] = None, status: WorkStatus = WorkStatus.PENDING) -> Optional[int]:
        """
        새 작업을 큐에 추가합니다.
        중복된 frfr_id와 analysis_id 조합은 추가되지 않습니다.
        
        Args:
            work_id: Work Queue ID
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            video_url: 비디오 URL
            workspace: 작업 디렉토리 경로 (선택사항)
            status: 초기 작업 상태 (기본값: PENDING)
            
        Returns:
            작업 ID 또는 None (중복된 경우)
        """
        try:
            job_id = self.job_queue.add_job(work_id, frfr_id, analysis_id, video_url, workspace, status)
            if job_id:
                logger.info(f"Job added successfully: job_id={job_id}, frfr_id={frfr_id}, analysis_id={analysis_id}, workspace={workspace}")
            else:
                logger.warning(f"Failed to add job (possibly duplicate): frfr_id={frfr_id}, analysis_id={analysis_id}")
            return job_id
        except Exception as e:
            logger.error(f"Error adding job: {str(e)}")
            raise
    
    def get_next_job(self) -> Optional[Dict[str, Any]]:
        """
        FIFO 순서로 다음 pending 상태의 작업을 가져오고 processing으로 변경합니다.
        
        Returns:
            {'job_id': int, 'frfr_id': str} 또는 None (pending 작업이 없는 경우)
        """
        try:
            job_info = self.job_queue.pop_next_job()
            if job_info:
                logger.info(f"Next job retrieved: job_id={job_info['job_id']}, frfr_id={job_info['frfr_id']}")
            else:
                logger.info("No pending jobs available")
            return job_info
        except Exception as e:
            logger.error(f"Error getting next job: {str(e)}")
            raise

    
    def get_job_by_id(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        작업 ID로 작업 정보를 조회합니다.
        
        Args:
            job_id: 작업 ID
            
        Returns:
            작업 정보 딕셔너리 또는 None (작업이 없는 경우)
        """
        try:
            result = self.job_queue.get_job_by_id(job_id)
            if result:
                logger.info(f"Job retrieved: job_id={job_id}")
            else:
                logger.warning(f"Job not found: job_id={job_id}")
            return result
        except Exception as e:
            logger.error(f"Error getting job: {str(e)}")
            raise
    
    def get_jobs_by_status(self, status: WorkStatus) -> List[Dict[str, Any]]:
        """
        특정 상태의 모든 작업을 조회합니다.
        
        Args:
            status: 조회할 작업 상태 (JobStatus enum)
            
        Returns:
            작업 정보 딕셔너리 리스트
        """
        try:
            status_value = status.value if isinstance(status, WorkStatus) else status
            result = self.job_queue.get_jobs_by_status(status_value)
            logger.info(f"Jobs retrieved with status={status_value}, count={len(result)}")
            return result
        except Exception as e:
            logger.error(f"Error getting jobs by status: {str(e)}")
            raise
    
    def update_job_status(self, job_id: int, status: WorkStatus) -> bool:
        """
        작업의 상태를 업데이트합니다.
        
        Args:
            job_id: 작업 ID
            status: 변경할 작업 상태 (JobStatus enum)
            
        Returns:
            성공 여부
        """
        try:
            status_value = status.value if isinstance(status, WorkStatus) else status
            result = self.job_queue.update_job_status(job_id, status_value)
            return result
        except Exception as e:
            logger.error(f"Error updating job status: {str(e)}")
            raise

    def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        특정 작업의 상태를 조회합니다.
        
        Args:
            job_id: 작업 ID
            
        Returns:
            작업의 상태 정보 또는 None
        """
        try:
            result = self.job_queue.get_job_status(job_id)
            if result:
                logger.info(f"Job status retrieved: job_id={job_id}, status={result['status']}")
            else:
                logger.warning(f"Job not found: job_id={job_id}")
            return result
        except Exception as e:
            logger.error(f"Error getting job status: {str(e)}")
            raise
    
    def has_pending_jobs(self) -> bool:
        """
        Pending 상태의 작업이 존재하는지 확인합니다.
        
        Returns:
            Pending 작업 존재 여부

        """
        try:
            pending_jobs = self.get_jobs_by_status(WorkStatus.PENDING)
            result = len(pending_jobs) > 0
            logger.info(f"Pending jobs check: has_pending={result}, count={len(pending_jobs)}")
            return result
        except Exception as e:
            logger.error(f"Error checking pending jobs: {str(e)}")
            raise
    
    def count_jobs_by_status(self, status: WorkStatus) -> int:
        """
        특정 상태의 작업 개수를 조회합니다.
        
        Args:
            status: 조회할 작업 상태 (JobStatus enum)
            
        Returns:
            해당 상태의 작업 개수

        """
        try:
            status_value = status.value if isinstance(status, WorkStatus) else status
            count = self.job_queue.count_jobs_by_status(status_value)
            logger.info(f"Job count by status: status={status_value}, count={count}")
            return count
        except Exception as e:
            logger.error(f"Error counting jobs by status: {str(e)}")
            raise
    
    def get_all_jobs(self) -> List[Dict[str, Any]]:
        """
        모든 작업 정보를 조회합니다.
        
        Returns:
            모든 작업 정보 리스트
        """
        try:
            jobs = self.job_queue.get_all_jobs()
            logger.info(f"Retrieved all jobs: count={len(jobs)}")
            return jobs
        except Exception as e:
            logger.error(f"Error getting all jobs: {str(e)}")
            raise

    def delete_job(self, job_id: int) -> bool:
        """
        작업을 삭제합니다.
        
        Args:
            job_id: 삭제할 작업 ID
            
        Returns:
            삭제 성공 여부
        """
        try:
            success = self.job_queue.delete_job(job_id)
            if success:
                logger.info(f"Job deleted: job_id={job_id}")
            else:
                logger.warning(f"Failed to delete job: job_id={job_id}")
            return success
        except Exception as e:
            logger.error(f"Error deleting job {job_id}: {str(e)}")
            raise