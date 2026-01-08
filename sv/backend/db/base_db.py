from contextlib import contextmanager
from sv.backend.db.connection_manager import DBConnectionManager

class BaseDB:
    """
    Database 테이블 관리를 위한 베이스 클래스
    
    DBConnectionManager를 통해 DB 연결을 공유하며,
    각 서브클래스는 자신의 테이블만 관리합니다.
    """
    
    def __init__(self, db_path: str = "jobs.db"):
        """
        BaseDB 초기화
        
        Args:
            db_path: 데이터베이스 파일 경로
        """
        # Singleton으로 관리되는 Connection Manager 획득
        self.connection_manager = DBConnectionManager(db_path)
        self.db_path = db_path

    @contextmanager
    def _conn(self):
        """데이터베이스 연결 컨텍스트 매니저 (Connection Manager 위임)"""
        with self.connection_manager.get_connection() as conn:
            yield conn

    def _init_db(self):
        """데이터베이스 초기화 (subclass에서 override)"""
        raise NotImplementedError("Subclass must implement _init_db()")
    
    def _get_table_name(self) -> str:
        """테이블 이름 반환 (subclass에서 override)"""
        raise NotImplementedError("Subclass must implement _get_table_name()")