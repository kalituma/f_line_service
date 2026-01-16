import sqlite3
from sv import DEFAULT_JOB_QUEUE_DB
from sv.utils.logger import setup_logger, setup_common_logger

logger = setup_logger(__name__)

def initialize_logger() -> None:
    """로거 초기화"""
    setup_common_logger(None)

def drop_tables() -> bool:
    """
    job_queue, tasks, work_queue 테이블이 존재할 경우 DROP 합니다.
    
    Returns:
        성공 여부
    """
    initialize_logger()
    
    db_path = DEFAULT_JOB_QUEUE_DB
    
    logger.info("="*80)
    logger.info(f"데이터베이스 테이블 DROP 작업 시작")
    logger.info(f"DB 경로: {db_path}")
    logger.info("="*80)
    logger.info("")
    
    try:
        # 데이터베이스 연결
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 드롭할 테이블 목록
        tables_to_drop = ['job_queue', 'work_queue']
        
        logger.info("현재 존재하는 테이블 확인 중...")
        logger.info("")
        
        # 현재 존재하는 테이블 확인
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"데이터베이스의 기존 테이블: {existing_tables}")
        logger.info("")
        
        # 각 테이블 드롭
        logger.info("="*80)
        logger.info("테이블 DROP 중...")
        logger.info("="*80)
        logger.info("")
        
        dropped_count = 0
        for table_name in tables_to_drop:
            if table_name in existing_tables:
                try:
                    # 외래키 제약 비활성화
                    cursor.execute("PRAGMA foreign_keys = OFF")
                    
                    # 테이블 드롭
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    
                    # 외래키 제약 재활성화
                    cursor.execute("PRAGMA foreign_keys = ON")
                    
                    logger.info(f"✓ 테이블 '{table_name}' 드롭됨")
                    dropped_count += 1
                except Exception as e:
                    logger.error(f"✗ 테이블 '{table_name}' 드롭 실패: {str(e)}", exc_info=True)
            else:
                logger.info(f"⊘ 테이블 '{table_name}'는 존재하지 않음 (스킵)")
        
        logger.info("")
        
        # 변경사항 커밋
        conn.commit()
        
        # 인덱스 확인
        logger.info("="*80)
        logger.info("인덱스 정리 중...")
        logger.info("="*80)
        logger.info("")
        
        # 관련 인덱스 확인
        indexes_to_check = [
            'idx_job_status',
            'idx_job_work_id',
            'idx_task_job',
            'idx_work_created_at',
            'idx_work_status'
        ]
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        existing_indexes = [row[0] for row in cursor.fetchall()]
        
        dropped_indexes = 0
        for index_name in indexes_to_check:
            if index_name in existing_indexes:
                try:
                    cursor.execute(f"DROP INDEX IF EXISTS {index_name}")
                    logger.info(f"✓ 인덱스 '{index_name}' 드롭됨")
                    dropped_indexes += 1
                except Exception as e:
                    logger.error(f"✗ 인덱스 '{index_name}' 드롭 실패: {str(e)}", exc_info=True)
        
        if dropped_indexes == 0:
            logger.info("⊘ 관련 인덱스가 없음 (스킵)")
        
        conn.commit()
        
        # 연결 종료
        conn.close()
        
        # 결과 요약
        logger.info("")
        logger.info("="*80)
        logger.info(f"✅ 완료!")
        logger.info(f"   - 드롭된 테이블: {dropped_count}개 ({', '.join(t for t in tables_to_drop if t in existing_tables)})")
        logger.info(f"   - 드롭된 인덱스: {dropped_indexes}개")
        logger.info("="*80)
        
        return True
        
    except sqlite3.Error as e:
        logger.error(f"❌ SQLite 에러 발생: {str(e)}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"❌ 예상치 못한 에러 발생: {str(e)}", exc_info=True)
        return False


def main():
    """메인 함수"""
    try:
        success = drop_tables()
        if not success:
            logger.error("테이블 드롭 작업 중 오류가 발생했습니다")
            exit(1)
    except Exception as e:
        logger.error(f"메인 함수 실행 중 에러: {str(e)}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
