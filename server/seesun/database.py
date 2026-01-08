"""
분석 전송 스케줄러용 데이터베이스 헬퍼.
"""
from contextlib import contextmanager
from typing import Dict, Iterable, List, Optional

import psycopg2
from loguru import logger
from psycopg2.extras import RealDictCursor

from config import settings


@contextmanager
def get_connection():
    """Postgres 연결 컨텍스트를 제공한다."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )
        yield conn
    except Exception as exc:
        logger.error(f"데이터베이스 연결 오류: {exc}")
        raise
    finally:
        if conn:
            conn.close()


def fetch_group001_pending() -> List[Dict]:
    """
    전송을 기다리는 분석 그룹(GROUP_001)을 생성일 오름차순으로 반환한다.
    """
    query = """
        SELECT id, event_cd, heli_callsign, analysis_id, analysis_status, created_at, updated_at
        FROM heli.analysis_info
        WHERE analysis_status = 'GROUP_001'
        ORDER BY created_at ASC
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]


def fetch_statuses(analysis_ids: Iterable[str]) -> Dict[str, Optional[str]]:
    """
    주어진 분석 ID 목록에 대한 analysis_id -> analysis_status 매핑을 반환한다.
    """
    ids = list(analysis_ids)
    if not ids:
        return {}

    query = """
        SELECT analysis_id, analysis_status
        FROM heli.analysis_info
        WHERE analysis_id = ANY(%s)
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (ids,))
            results = cursor.fetchall()
            return {row[0]: row[1] for row in results}


def mark_as_in_progress(analysis_id: str) -> bool:
    """
    전송된 GROUP_001 분석 그룹을 GROUP_002로 변경한다.
    행이 수정되면 True를 반환한다.
    """
    if not settings.mark_group002_on_dispatch:
        return False

    query = """
        UPDATE heli.analysis_info
        SET analysis_status = 'GROUP_002', updated_at = NOW()
        WHERE analysis_id = %s AND analysis_status = 'GROUP_001'
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (analysis_id,))
            updated = cursor.rowcount > 0
            if updated:
                conn.commit()
            return updated

