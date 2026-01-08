import logging
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

from sv.backend.db.job_queue import JobQueue, JobStatus

logger = logging.getLogger(__name__)


class JobQueueRequest(BaseModel):
    """작업 큐 요청 모델"""
    frfr_id: str
    analysis_id: str


class JobStatusResponse(BaseModel):
    """작업 상태 응답 모델"""
    job_id: Optional[int]
    frfr_id: str
    analysis_id: str
    status: str


class TaskInitRequest(BaseModel):
    """작업 태스크 초기화 요청 모델"""
    job_id: int
    task_names: List[str]


class JobQueueService:
    """JobQueue를 활용한 작업 큐 관리 서비스"""
    
    def __init__(self, db_path: str = "jobs.db"):
        """
        서비스 초기화
        
        Args:
            db_path: JobQueue 데이터베이스 경로 (기본값: jobs.db)
        """
        self.job_queue = JobQueue(db_path)
        self.job_queue._init_db()
        logger.info(f"JobQueueService initialized with db_path={db_path}")
    
    def add_job(self, frfr_id: str, analysis_id: str, status: JobStatus = JobStatus.PENDING) -> Optional[int]:
        """
        새 작업을 큐에 추가합니다.
        중복된 frfr_id와 analysis_id 조합은 추가되지 않습니다.
        
        Args:
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            status: 초기 작업 상태 (기본값: PENDING)
            
        Returns:
            작업 ID 또는 None (중복된 경우)
            
        Examples:
            >>> service = JobQueueService()
            >>> job_id = service.add_job("FIRE_001", "ANALYSIS_001")
            >>> print(job_id)  # 1
        """
        try:
            job_id = self.job_queue.add_job(frfr_id, analysis_id, status)
            if job_id:
                logger.info(f"Job added successfully: job_id={job_id}, frfr_id={frfr_id}, analysis_id={analysis_id}")
            else:
                logger.warning(f"Failed to add job (possibly duplicate): frfr_id={frfr_id}, analysis_id={analysis_id}")
            return job_id
        except Exception as e:
            logger.error(f"Error adding job: {str(e)}")
            raise
    
    def get_next_job(self) -> Optional[int]:
        """
        FIFO 순서로 다음 pending 상태의 작업을 가져오고 processing으로 변경합니다.
        
        Returns:
            처리할 다음 job_id 또는 None (pending 작업이 없는 경우)
            
        Examples:
            >>> service = JobQueueService()
            >>> service.add_job("FIRE_001", "ANALYSIS_001")
            >>> job_id = service.get_next_job()
            >>> print(job_id)  # 1
        """
        try:
            job_id = self.job_queue.pop_next_job()
            if job_id:
                logger.info(f"Next job retrieved: job_id={job_id}")
            else:
                logger.info("No pending jobs available")
            return job_id
        except Exception as e:
            logger.error(f"Error getting next job: {str(e)}")
            raise
    
    def initialize_tasks(self, job_id: int, task_names: List[str]) -> bool:
        """
        작업에 대한 태스크를 초기화합니다.
        
        Args:
            job_id: 작업 ID
            task_names: 태스크 이름 리스트
            
        Returns:
            성공 여부
            
        Examples:
            >>> service = JobQueueService()
            >>> service.initialize_tasks(1, ["extract", "analyze", "save"])
            >>> True
        """
        try:
            result = self.job_queue.init_tasks(job_id, task_names)
            if result:
                logger.info(f"Tasks initialized: job_id={job_id}, task_count={len(task_names)}")
            else:
                logger.warning(f"Failed to initialize tasks: job_id={job_id}")
            return result
        except Exception as e:
            logger.error(f"Error initializing tasks: {str(e)}")
            raise
    
    def add_job_with_tasks(self, frfr_id: str, analysis_id: str, task_names: List[str]) -> Optional[int]:
        """
        작업을 추가하고 바로 태스크를 초기화하는 편의 메서드입니다.
        
        Args:
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            task_names: 태스크 이름 리스트
            
        Returns:
            생성된 작업 ID 또는 None (실패한 경우)
            
        Examples:
            >>> service = JobQueueService()
            >>> job_id = service.add_job_with_tasks(
            ...     "FIRE_001", 
            ...     "ANALYSIS_001",
            ...     ["extract", "analyze", "save"]
            ... )
            >>> print(job_id)  # 1
        """
        try:
            # 작업 추가
            job_id = self.job_queue.add_job(frfr_id, analysis_id, JobStatus.PENDING)
            if not job_id:
                logger.warning(f"Failed to add job: frfr_id={frfr_id}, analysis_id={analysis_id}")
                return None
            
            # 태스크 초기화
            if not self.job_queue.init_tasks(job_id, task_names):
                logger.error(f"Failed to initialize tasks for job_id={job_id}")
                return None
            
            logger.info(f"Job with tasks added successfully: job_id={job_id}")
            return job_id
        except Exception as e:
            logger.error(f"Error in add_job_with_tasks: {str(e)}")
            raise
    
    def get_job_by_id(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        작업 ID로 작업 정보를 조회합니다.
        
        Args:
            job_id: 작업 ID
            
        Returns:
            작업 정보 딕셔너리 또는 None (작업이 없는 경우)
            
        Examples:
            >>> service = JobQueueService()
            >>> job_info = service.get_job_by_id(1)
            >>> print(job_info)  # {'job_id': 1, 'frfr_id': 'FIRE_001', ...}
        """
        try:
            with self.job_queue._conn() as conn:
                row = conn.execute(
                    'SELECT * FROM job_queue WHERE job_id = ?',
                    (job_id,)
                ).fetchone()
                
                if row:
                    result = dict(row)
                    logger.info(f"Job retrieved: job_id={job_id}")
                    return result
                else:
                    logger.warning(f"Job not found: job_id={job_id}")
                    return None
        except Exception as e:
            logger.error(f"Error getting job: {str(e)}")
            raise
    
    def get_jobs_by_status(self, status: JobStatus) -> List[Dict[str, Any]]:
        """
        특정 상태의 모든 작업을 조회합니다.
        
        Args:
            status: 조회할 작업 상태 (JobStatus enum)
            
        Returns:
            작업 정보 딕셔너리 리스트
            
        Examples:
            >>> service = JobQueueService()
            >>> pending_jobs = service.get_jobs_by_status(JobStatus.PENDING)
            >>> print(len(pending_jobs))  # 3
        """
        try:
            status_value = status.value if isinstance(status, JobStatus) else status
            with self.job_queue._conn() as conn:
                rows = conn.execute(
                    'SELECT * FROM job_queue WHERE status = ? ORDER BY created_at ASC',
                    (status_value,)
                ).fetchall()
                
                result = [dict(row) for row in rows]
                logger.info(f"Jobs retrieved with status={status_value}, count={len(result)}")
                return result
        except Exception as e:
            logger.error(f"Error getting jobs by status: {str(e)}")
            raise
    
    def update_job_status(self, job_id: int, status: JobStatus) -> bool:
        """
        작업의 상태를 업데이트합니다.
        
        Args:
            job_id: 작업 ID
            status: 변경할 작업 상태 (JobStatus enum)
            
        Returns:
            성공 여부
            
        Examples:
            >>> service = JobQueueService()
            >>> success = service.update_job_status(1, JobStatus.COMPLETED)
            >>> print(success)  # True
        """
        try:
            import time
            status_value = status.value if isinstance(status, JobStatus) else status
            with self.job_queue._conn() as conn:
                conn.execute(
                    'UPDATE job_queue SET status = ?, created_at = ? WHERE job_id = ?',
                    (status_value, time.time(), job_id)
                )
            logger.info(f"Job status updated: job_id={job_id}, status={status_value}")
            return True
        except Exception as e:
            logger.error(f"Error updating job status: {str(e)}")
            raise
    
    def get_job_tasks(self, job_id: int) -> List[Dict[str, Any]]:
        """
        작업의 모든 태스크를 조회합니다.
        
        Args:
            job_id: 작업 ID
            
        Returns:
            태스크 정보 딕셔너리 리스트
            
        Examples:
            >>> service = JobQueueService()
            >>> tasks = service.get_job_tasks(1)
            >>> for task in tasks:
            ...     print(f"{task['task_name']}: {task['status']}")
        """
        try:
            with self.job_queue._conn() as conn:
                rows = conn.execute(
                    'SELECT * FROM tasks WHERE job_id = ? ORDER BY seq ASC',
                    (job_id,)
                ).fetchall()
                
                result = [dict(row) for row in rows]
                logger.info(f"Tasks retrieved for job_id={job_id}, count={len(result)}")
                return result
        except Exception as e:
            logger.error(f"Error getting job tasks: {str(e)}")
            raise
    
    def update_task_status(self, task_id: int, status: JobStatus) -> bool:
        """
        태스크의 상태를 업데이트합니다.
        
        Args:
            task_id: 태스크 ID
            status: 변경할 태스크 상태 (JobStatus enum)
            
        Returns:
            성공 여부
            
        Examples:
            >>> service = JobQueueService()
            >>> success = service.update_task_status(1, JobStatus.COMPLETED)
            >>> print(success)  # True
        """
        try:
            import time
            status_value = status.value if isinstance(status, JobStatus) else status
            with self.job_queue._conn() as conn:
                conn.execute(
                    'UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?',
                    (status_value, time.time(), task_id)
                )
            logger.info(f"Task status updated: task_id={task_id}, status={status_value}")
            return True
        except Exception as e:
            logger.error(f"Error updating task status: {str(e)}")
            raise
    
    def delete_job(self, job_id: int) -> bool:
        """
        작업을 삭제합니다 (관련된 태스크도 함께 삭제됩니다).
        
        Args:
            job_id: 작업 ID
            
        Returns:
            성공 여부
            
        Examples:
            >>> service = JobQueueService()
            >>> success = service.delete_job(1)
            >>> print(success)  # True
        """
        try:
            with self.job_queue._conn() as conn:
                # 관련된 태스크 먼저 삭제
                conn.execute('DELETE FROM tasks WHERE job_id = ?', (job_id,))
                # 작업 삭제
                conn.execute('DELETE FROM job_queue WHERE job_id = ?', (job_id,))
            logger.info(f"Job deleted: job_id={job_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting job: {str(e)}")
            raise
    
    def get_all_jobs(self) -> List[Dict[str, Any]]:
        """
        모든 작업을 조회합니다.
        
        Returns:
            작업 정보 딕셔너리 리스트
            
        Examples:
            >>> service = JobQueueService()
            >>> all_jobs = service.get_all_jobs()
            >>> print(len(all_jobs))  # 10
        """
        try:
            with self.job_queue._conn() as conn:
                rows = conn.execute(
                    'SELECT * FROM job_queue ORDER BY created_at DESC'
                ).fetchall()
                
                result = [dict(row) for row in rows]
                logger.info(f"All jobs retrieved, count={len(result)}")
                return result
        except Exception as e:
            logger.error(f"Error getting all jobs: {str(e)}")
            raise

