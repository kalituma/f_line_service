import time
from typing import List, Callable, Dict, Any
from enum import Enum
from sv.backend.service.job_queue_service import JobQueueService
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)


class ChangeEventType(Enum):
    """변경 이벤트 타입"""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    PENDING_JOBS_DETECTED = "pending_jobs_detected"


class DBChangeEvent:
    """데이터베이스 변경 이벤트"""
    
    def __init__(
        self,
        table_name: str,
        event_type: ChangeEventType,
        data: Dict[str, Any],
        timestamp: float = None
    ):
        self.table_name = table_name
        self.event_type = event_type
        self.data = data
        self.timestamp = timestamp or time.time()
    
    def __repr__(self):
        return (
            f"DBChangeEvent(table={self.table_name}, "
            f"type={self.event_type.value}, "
            f"data={self.data})"
        )

class DBChangeListener:
    """데이터베이스 변경 감지 리스너 (이벤트 기반)"""
    
    def __init__(self, poll_interval: float = 2.0):
        """
        Args:
            poll_interval: 폴링 간격 (초)
        """        
        
        self.job_queue_service = JobQueueService()
        self.poll_interval = poll_interval
        self.callbacks: List[Callable[[DBChangeEvent], None]] = []
        
        logger.info(f"DBChangeListener initialized with poll_interval={poll_interval}")
    
    def on_change(self, callback: Callable[[DBChangeEvent], None]):
        """
        변경 이벤트 콜백 등록
        
        Args:
            callback: 이벤트 발생 시 실행할 콜백 함수
        """
        self.callbacks.append(callback)
        logger.info(f"Callback registered: {callback.__name__}")
    
    def _emit_event(self, event: DBChangeEvent):
        """이벤트 발생"""
        logger.info(f"Emitting event: {event}")
        for callback in self.callbacks:
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Error in callback {callback.__name__}: {str(e)}", exc_info=True)
    
    def check_pending_jobs(self, status: str = "pending") -> List[DBChangeEvent]:
        """
        Job Queue 테이블에서 pending 상태의 행 개수가 0 이상일 경우 이벤트 발생
        Args:
            status: 감시할 상태값 (기본값: "pending")
        Returns:
            감지된 변경 이벤트 리스트
        """
        events = []
        table_key = f"job_queue_{status}"
        
        try:
            # 서비스 레이어를 통해 pending jobs 조회
            pending_jobs = self.job_queue_service.get_jobs_by_status(status)
            current_count = len(pending_jobs)

            if current_count > 0:
                event = DBChangeEvent(
                    table_name="job_queue",
                    event_type=ChangeEventType.PENDING_JOBS_DETECTED,
                    data={
                        "status": status,
                        "count": current_count,
                        "jobs": pending_jobs
                    }
                )
                events.append(event)
                self._emit_event(event)
                logger.info(
                    f"Pending jobs detected in job_queue: "
                    f"count={current_count}, status={status}"
                )
            else:
                # pending 작업이 없음
                logger.info(f"No pending jobs found in job_queue (status={status})")
            
        except Exception as e:
            logger.error(
                f"Error checking pending jobs for status '{status}': {str(e)}",
                exc_info=True
            )
        
        return events
    
    def check_jobs_by_count(self, status: str = "pending") -> int:
        """
        특정 상태의 Job 개수를 조회합니다
        
        Args:
            status: 조회할 상태값
            
        Returns:
            해당 상태의 Job 개수
        """
        try:
            from sv.backend.db.job_queue_db import JobStatus
            
            # 상태값을 JobStatus enum으로 변환
            status_enum = JobStatus(status)
            count = self.job_queue_service.count_jobs_by_status(status_enum)
            logger.info(f"Job count check: status={status}, count={count}")
            return count
        except Exception as e:
            logger.error(f"Error counting jobs by status '{status}': {str(e)}", exc_info=True)
            return 0
    
    def has_pending_jobs(self) -> bool:
        """
        Pending 상태의 Job이 존재하는지 확인합니다
        
        Returns:
            Pending Job 존재 여부
        """
        try:
            return self.job_queue_service.has_pending_jobs()
        except Exception as e:
            logger.error(f"Error checking if pending jobs exist: {str(e)}", exc_info=True)
            return False

