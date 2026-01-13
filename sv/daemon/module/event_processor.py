from typing import Callable, Optional
from sv.daemon.module.db_change_listener import DBChangeListener, DBChangeEvent, ChangeEventType
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class EventProcessor:
    """DB 변경 이벤트 처리"""
    
    def __init__(
        self,
        poll_interval: float = 2.0,
        on_job_created: Optional[Callable] = None
    ):
        """
        Args:
            poll_interval: 폴링 간격
            db_path: 데이터베이스 경로
        """
        self.db_listener = DBChangeListener(poll_interval)
        self.db_listener.on_change(self._handle_db_change)
        self.on_job_created = on_job_created
    
    def _handle_db_change(self, event: DBChangeEvent) -> None:
        """
        DB 변경 이벤트 내부 핸들러
        
        Args:
            event: DB 변경 이벤트
        """
        logger.info(f"DB Change detected: {event}")

        # Job Queue INSERT 이벤트 처리
        if event.table_name == "job_queue" and event.event_type == ChangeEventType.PENDING_JOBS_DETECTED:
            logger.info(f"✓ New job detected: {event.data}")
            if self.on_job_created:
                try:
                    # 단순히 콜백 호출 (Ray가 알아서 비동기 처리)
                    self.on_job_created()
                except Exception as e:
                    logger.error(f"Error in on_job_created callback: {str(e)}", exc_info=True)
    
    def check_changes(self) -> None:
        """
        Pending 상태의 Job 개수 확인                    
        """
        try:
            self.db_listener.check_pending_jobs()
        except Exception as e:
            logger.error(f"Error checking pending jobs: {str(e)}")

