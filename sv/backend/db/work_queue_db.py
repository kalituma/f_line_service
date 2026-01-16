from typing import Optional, List
import sqlite3

from sv.utils.logger import setup_logger
from sv.backend.db.base_db import BaseDB
from sv.backend.work_status import WorkStatus
from sv.utils.date_parsing import _format_timestamps
logger = setup_logger(__name__)

class WorkQueue(BaseDB):
    """Work 큐 관리 클래스"""
    
    def _get_table_name(self) -> str:
        """테이블 이름 반환"""
        return "work_queue"
    
    def _init_db(self):
        """Work Queue 테이블 초기화 (한 번만 실행)"""
        table_name = self._get_table_name()

        with self._conn() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS work_queue (
                    work_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    frfr_id TEXT NOT NULL,
                    analysis_id TEXT NOT NULL,                    
                    status TEXT DEFAULT 'pending',                    
                    created_at REAL
                );

                CREATE INDEX IF NOT EXISTS idx_work_status ON work_queue(status, created_at);
                CREATE INDEX IF NOT EXISTS idx_work_created_at ON work_queue(created_at);
            ''')

        logger.info(f"Table '{table_name}' initialized successfully")
        
    def add_work(self, frfr_id: str, analysis_id: str, status: WorkStatus = WorkStatus.PENDING) -> Optional[int]:
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
        status_value = status.value if isinstance(status, WorkStatus) else status
        
        with self._conn() as conn:
            try:
                cursor = conn.execute(
                    'INSERT INTO work_queue (frfr_id, analysis_id, status, created_at) VALUES (?, ?, ?, ?)',
                    (frfr_id, analysis_id, status_value, now)
                )
                logger.info(f"Work added: frfr_id={frfr_id}, analysis_id={analysis_id}, status={status_value}")
                return cursor.lastrowid
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed" in str(e):
                    logger.warning(f"Duplicate work: frfr_id={frfr_id}, analysis_id={analysis_id} (already exists)")
                else:
                    logger.error(f"Failed to add work: {frfr_id}/{analysis_id} - {str(e)}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error adding work: {str(e)}")
                return None

    def get_next_work(self) -> Optional[dict]:
        """
        FIFO로 다음 pending work 가져옵니다.
        
        Returns:
            {'work_id': int, 'frfr_id': str, 'analysis_id': str} 또는 None
        """
        with self._conn() as conn:
            row = conn.execute(
                'SELECT work_id, frfr_id, analysis_id FROM work_queue WHERE status = ? ORDER BY created_at ASC LIMIT 1',
                (WorkStatus.PENDING.value,)
            ).fetchone()
            
            if row:
                return {
                    'work_id': row['work_id'],
                    'frfr_id': row['frfr_id'],
                    'analysis_id': row['analysis_id']
                }
        return None
    
    def get_all_works(self) -> List[dict]:
        """모든 works 조회"""
        try:
            with self._conn() as conn:
                rows = conn.execute('SELECT * FROM work_queue ORDER BY created_at DESC').fetchall()
                return [_format_timestamps(dict(row)) for row in rows]
        except Exception as e:
            logger.error(f"Error getting all works: {str(e)}")
            return []
    
    def get_work_by_id(self, work_id: int) -> Optional[dict]:
        """Work ID로 work 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT * FROM work_queue WHERE work_id = ?',
                    (work_id,)
                ).fetchone()
                return _format_timestamps(dict(row)) if row else None
        except Exception as e:
            logger.error(f"Error getting work by id: {str(e)}")
            return None
    
    def get_works_by_frfr_id(self, frfr_id: str) -> List[dict]:
        """frfr_id로 works 조회"""
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    'SELECT * FROM work_queue WHERE frfr_id = ? ORDER BY created_at ASC',
                    (frfr_id,)
                ).fetchall()
                return [_format_timestamps(dict(row)) for row in rows]
        except Exception as e:
            logger.error(f"Error getting works by frfr_id: {str(e)}")
            return []
    
    def get_works_by_analysis_id(self, analysis_id: str) -> List[dict]:
        """analysis_id로 works 조회"""
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    'SELECT * FROM work_queue WHERE analysis_id = ? ORDER BY created_at ASC',
                    (analysis_id,)
                ).fetchall()
                return [_format_timestamps(dict(row)) for row in rows]
        except Exception as e:
            logger.error(f"Error getting works by analysis_id: {str(e)}")
            return []
    
    def get_works_by_status(self, status: str) -> List[dict]:
        """상태별로 works 조회"""
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    'SELECT * FROM work_queue WHERE status = ? ORDER BY created_at ASC',
                    (status,)
                ).fetchall()
                return [_format_timestamps(dict(row)) for row in rows]
        except Exception as e:
            logger.error(f"Error getting works by status: {str(e)}")
            return []
    
    def update_work_status(self, work_id: int, status: str) -> bool:
        """Work 상태 업데이트"""
        import time
        try:
            with self._conn() as conn:
                conn.execute(
                    'UPDATE work_queue SET status = ? WHERE work_id = ?',
                    (status, work_id)
                )
            logger.info(f"Work status updated: work_id={work_id}, status={status} at {time.time()}")
            return True
        except Exception as e:
            logger.error(f"Error updating work status: {str(e)}")
            return False
    
    def get_work_status(self, work_id: int) -> Optional[dict]:
        """Work 상태 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT * FROM work_queue WHERE work_id = ?',
                    (work_id,)
                ).fetchone()
                return _format_timestamps(dict(row)) if row else None
        except Exception as e:
            logger.error(f"Error getting work status: {str(e)}")
            return None
    
    def count_works_by_status(self, status: str) -> int:
        """상태별 work 개수 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT COUNT(*) as count FROM work_queue WHERE status = ?',
                    (status,)
                ).fetchone()
                return row['count'] if row else 0
        except Exception as e:
            logger.error(f"Error counting works by status: {str(e)}")
            return 0
    
    def delete_work(self, work_id: int) -> bool:
        """Work 삭제"""
        try:
            with self._conn() as conn:
                conn.execute('DELETE FROM work_queue WHERE work_id = ?', (work_id,))
            logger.info(f"Work deleted: work_id={work_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting work: {str(e)}")
            return False
    
    def count_all_works(self) -> int:
        """전체 work 개수 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT COUNT(*) as count FROM work_queue'
                ).fetchone()
                return row['count'] if row else 0
        except Exception as e:
            logger.error(f"Error counting all works: {str(e)}")
            return 0
    
    def count_works_by_frfr_id(self, frfr_id: str) -> int:
        """frfr_id별 work 개수 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT COUNT(*) as count FROM work_queue WHERE frfr_id = ?',
                    (frfr_id,)
                ).fetchone()
                return row['count'] if row else 0
        except Exception as e:
            logger.error(f"Error counting works by frfr_id: {str(e)}")
            return 0
