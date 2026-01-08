from typing import Optional, List
import sqlite3
from enum import Enum

from sv.utils.logger import setup_logger
from sv.backend.db.base_db import BaseDB

logger = setup_logger(__name__)

class TaskStatus(Enum):
    """작업 상태 Enum"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    
    def __str__(self):
        return self.value

class TaskQueue(BaseDB):
    """Task 관리 클래스"""
    
    def _get_table_name(self) -> str:
        """테이블 이름 반환"""
        return "tasks"
    
    def _init_db(self):
        """Task 테이블 초기화 (한 번만 실행)"""
        table_name = self._get_table_name()

        with self._conn() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER,
                    workspace TEXT,
                    seq INTEGER,
                    updated_at REAL,
                    status TEXT DEFAULT 'pending',
                    FOREIGN KEY (job_id) REFERENCES job_queue(job_id)
                );

                CREATE INDEX IF NOT EXISTS idx_task_job ON tasks(job_id, seq);
            ''')

        logger.info(f"Table '{table_name}' initialized successfully")
    
    def init_tasks(self, job_id: int, task_names: List[str]) -> bool:
        """Task 초기화"""
        import time
        with self._conn() as conn:
            for task_name in task_names:
                try:
                    conn.execute(
                        'INSERT INTO tasks (job_id, workspace, status, updated_at) VALUES (?, ?, ?, ?)',
                        (job_id, task_name, TaskStatus.PENDING.value, time.time())
                    )
                except sqlite3.IntegrityError as e:
                    if "UNIQUE constraint failed" in str(e):
                        logger.warning(f"Duplicate task: job_id={job_id}, task_name={task_name} (already exists)")
                    else:
                        logger.error(f"Failed to add task: {job_id}/{task_name} - {str(e)}")
                    return False
            return True
    
    def get_all_tasks(self) -> List[dict]:
        """모든 tasks 조회"""
        try:
            with self._conn() as conn:
                rows = conn.execute('SELECT * FROM tasks ORDER BY job_id, seq').fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting all tasks: {str(e)}")
            return []
    
    def get_job_tasks(self, job_id: int) -> List[dict]:
        """특정 Job의 모든 tasks 조회"""
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    'SELECT * FROM tasks WHERE job_id = ? ORDER BY seq ASC',
                    (job_id,)
                ).fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting job tasks: {str(e)}")
            return []
    
    def get_task_by_id(self, task_id: int) -> Optional[dict]:
        """Task ID로 task 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT * FROM tasks WHERE task_id = ?',
                    (task_id,)
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting task by id: {str(e)}")
            return None
    
    def get_tasks_by_status(self, status: str) -> List[dict]:
        """상태별로 tasks 조회"""
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    'SELECT * FROM tasks WHERE status = ? ORDER BY updated_at ASC',
                    (status,)
                ).fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting tasks by status: {str(e)}")
            return []
    
    def update_task_status(self, task_id: int, status: str) -> bool:
        """Task 상태 업데이트"""
        import time
        try:
            with self._conn() as conn:
                conn.execute(
                    'UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?',
                    (status, time.time(), task_id)
                )
            logger.info(f"Task status updated: task_id={task_id}, status={status}")
            return True
        except Exception as e:
            logger.error(f"Error updating task status: {str(e)}")
            return False
    
    def delete_task(self, task_id: int) -> bool:
        """Task 삭제"""
        try:
            with self._conn() as conn:
                conn.execute('DELETE FROM tasks WHERE task_id = ?', (task_id,))
            logger.info(f"Task deleted: task_id={task_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting task: {str(e)}")
            return False
    
    def delete_job_tasks(self, job_id: int) -> bool:
        """특정 Job의 모든 tasks 삭제"""
        try:
            with self._conn() as conn:
                conn.execute('DELETE FROM tasks WHERE job_id = ?', (job_id,))
            logger.info(f"Tasks deleted for job_id={job_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting job tasks: {str(e)}")
            return False
    
    def get_task_status(self, task_id: int) -> Optional[dict]:
        """Task 상태 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT * FROM tasks WHERE task_id = ?',
                    (task_id,)
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting task status: {str(e)}")
            return None
    
    def count_tasks_by_status(self, status: str) -> int:
        """상태별 task 개수 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT COUNT(*) as count FROM tasks WHERE status = ?',
                    (status,)
                ).fetchone()
                return row['count'] if row else 0
        except Exception as e:
            logger.error(f"Error counting tasks by status: {str(e)}")
            return 0
    
    def count_job_tasks(self, job_id: int) -> int:
        """특정 Job의 task 개수 조회"""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT COUNT(*) as count FROM tasks WHERE job_id = ?',
                    (job_id,)
                ).fetchone()
                return row['count'] if row else 0
        except Exception as e:
            logger.error(f"Error counting job tasks: {str(e)}")
            return 0

