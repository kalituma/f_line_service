from typing import Optional, List
import sqlite3
from enum import Enum

from sv.utils.logger import setup_logger
from sv.backend.db.base_db import BaseDB

logger = setup_logger(__name__)

class JobStatus(Enum):
    """작업 상태 Enum"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    
    def __str__(self):
        return self.value

# ==================== JobQueue Class ====================

class JobQueue(BaseDB):
    """Job 큐 관리 클래스"""
    
    def _get_table_name(self) -> str:
        """테이블 이름 반환"""
        return "job_queue"
    
    def _init_db(self):
        """Job Queue 테이블 초기화 (한 번만 실행)"""
        table_name = self._get_table_name()

        with self._conn() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS job_queue (
                    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    frfr_id TEXT NOT NULL,
                    analysis_id TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at REAL,
                    updated_at REAL,
                    UNIQUE(frfr_id, analysis_id)
                );

                CREATE INDEX IF NOT EXISTS idx_job_status ON job_queue(status, created_at);
            ''')

        logger.info(f"Table '{table_name}' initialized successfully")
        
    def add_job(self, frfr_id: str, analysis_id: str, status: JobStatus = JobStatus.PENDING) -> Optional[int]:
        """
        작업을 큐에 추가합니다.
        중복된 frfr_id와 analysis_id 조합은 추가되지 않습니다.
        
        Args:
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            status: 초기 작업 상태 (기본값: PENDING)
            
        Returns:
            작업 ID 또는 None (중복된 경우 None 반환)
        """
        import time
        now = time.time()
        
        # enum 값을 문자열로 변환
        status_value = status.value if isinstance(status, JobStatus) else status
        
        with self._conn() as conn:
            try:
                cursor = conn.execute(
                    'INSERT INTO job_queue (frfr_id, analysis_id, status, created_at) VALUES (?, ?, ?, ?)',
                    (frfr_id, analysis_id, status_value, now)
                )
                logger.info(f"Job added: frfr_id={frfr_id}, analysis_id={analysis_id}, status={status_value}")
                return cursor.lastrowid
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed" in str(e):
                    logger.warning(f"Duplicate job: frfr_id={frfr_id}, analysis_id={analysis_id} (already exists)")
                else:
                    logger.error(f"Failed to add job: {frfr_id}/{analysis_id} - {str(e)}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error adding job: {str(e)}")
                return None

    def pop_next_job(self) -> str | None:
        """FIFO로 다음 pending job 가져와서 processing으로 변경"""
        import time
        with self._conn() as conn:
            row = conn.execute(
                'SELECT job_id FROM job_queue WHERE status = ? ORDER BY created_at ASC LIMIT 1',
                (JobStatus.PENDING.value,)
            ).fetchone()
            
            if row:
                conn.execute(
                    'UPDATE job_queue SET status = ?, created_at = ? WHERE job_id = ?',
                    (JobStatus.PROCESSING.value, time.time(), row['job_id'])
                )
                return row['job_id']
        return None
    
    def get_all_jobs(self) -> List[dict]:
        """모든 jobs 조회"""
        try:
            with self._conn() as conn:
                rows = conn.execute('SELECT * FROM job_queue ORDER BY created_at DESC').fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting all jobs: {str(e)}")
            return []
    
    def get_job_by_id(self, job_id: int) -> Optional[dict]:
        """Job ID로 job 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT * FROM job_queue WHERE job_id = ?',
                    (job_id,)
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting job by id: {str(e)}")
            return None
    
    def get_jobs_by_status(self, status: str) -> List[dict]:
        """상태별로 jobs 조회"""
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    'SELECT * FROM job_queue WHERE status = ? ORDER BY created_at ASC',
                    (status,)
                ).fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting jobs by status: {str(e)}")
            return []
    
    def update_job_status(self, job_id: int, status: str) -> bool:
        """Job 상태 업데이트"""
        import time
        try:
            with self._conn() as conn:
                conn.execute(
                    'UPDATE job_queue SET status = ?, created_at = ? WHERE job_id = ?',
                    (status, time.time(), job_id)
                )
            logger.info(f"Job status updated: job_id={job_id}, status={status}")
            return True
        except Exception as e:
            logger.error(f"Error updating job status: {str(e)}")
            return False
    
    def delete_job(self, job_id: int) -> bool:
        """Job 삭제 (tasks는 task_db에서 처리)"""
        try:
            with self._conn() as conn:
                conn.execute('DELETE FROM job_queue WHERE job_id = ?', (job_id,))
            logger.info(f"Job deleted: job_id={job_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting job: {str(e)}")
            return False
    
    def get_job_status(self, job_id: int) -> Optional[dict]:
        """Job 상태 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT * FROM job_queue WHERE job_id = ?',
                    (job_id,)
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting job status: {str(e)}")
            return None
    
    def count_jobs_by_status(self, status: str) -> int:
        """상태별 job 개수 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT COUNT(*) as count FROM job_queue WHERE status = ?',
                    (status,)
                ).fetchone()
                return row['count'] if row else 0
        except Exception as e:
            logger.error(f"Error counting jobs by status: {str(e)}")
            return 0
