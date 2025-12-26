import logging
from tinydb import TinyDB, Query
from pathlib import Path
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

# 데이터베이스 파일 경로
DB_PATH = Path(__file__).parent.parent.parent.parent / "data" / "tinydb" / "wildfire.json"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

class SharedDatabase:
    """TinyDB 공유 인스턴스 관리"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db = TinyDB(str(db_path))
        logger.info(f"Shared database initialized at {db_path}")

    def get_table(self, table_name: str):
        """테이블 획득"""
        return self.db.table(table_name)

    def close(self):
        """데이터베이스 연결 종료"""
        self.db.close()
        logger.info("Database connection closed")


# 싱글톤 패턴으로 공유 데이터베이스 관리
_shared_db_instance = None

def get_shared_database() -> SharedDatabase:
    """공유 데이터베이스 싱글톤 인스턴스 반환"""
    global _shared_db_instance
    if _shared_db_instance is None:
        _shared_db_instance = SharedDatabase()
    return _shared_db_instance