import sqlite3
from contextlib import contextmanager
from threading import Lock
from typing import Dict
from pathlib import Path

class DBConnectionManager:
    """
    데이터베이스 연결을 관리하는 Singleton 클래스
    같은 db_path에 대해서는 항상 같은 인스턴스를 반환합니다.
    """
    _instances: Dict[str, 'DBConnectionManager'] = {}
    _lock = Lock()

    def __new__(cls, db_path: str):
        """db_path별로 단 하나의 인스턴스만 생성"""
        if db_path not in cls._instances:
            with cls._lock:
                if db_path not in cls._instances:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instances[db_path] = instance
                    Path(db_path).mkdir(parents=True, exist_ok=True)

        return cls._instances[db_path]

    def __init__(self, db_path: str):
        """초기화는 한 번만 수행"""
        if self._initialized:
            return
        
        self.db_path = db_path
        self._initialized = True

    @contextmanager
    def get_connection(self):
        """데이터베이스 연결 컨텍스트 매니저"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()