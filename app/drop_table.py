import sqlite3
import argparse
from sv import DEFAULT_JOB_QUEUE_DB
from sv.utils.logger import setup_logger, setup_common_logger

logger = setup_logger(__name__)

def initialize_logger() -> None:
    """로거 초기화"""
    setup_common_logger(None)

def drop_table(table_name: str) -> bool:
    """
    지정된 테이블을 DROP합니다.
    
    Args:
        table_name: DROP할 테이블 이름
        
    Returns:
        성공 여부
    """
    initialize_logger()
    
    db_path = DEFAULT_JOB_QUEUE_DB
    
    logger.info("="*80)
    logger.info(f"데이터베이스 테이블 DROP 작업 시작")
    logger.info(f"DB 경로: {db_path}")
    logger.info(f"대상 테이블: {table_name}")
    logger.info("="*80)
    logger.info("")
    
    try:
        # 데이터베이스 연결
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 현재 존재하는 테이블 확인
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"데이터베이스의 기존 테이블: {existing_tables}")
        logger.info("")
        
        # 테이블 존재 여부 확인
        if table_name not in existing_tables:
            logger.warning(f"⊘ 테이블 '{table_name}'는 존재하지 않습니다")
            logger.warning("="*80)
            conn.close()
            return False
        
        # 테이블 DROP
        logger.info("="*80)
        logger.info(f"테이블 '{table_name}' DROP 중...")
        logger.info("="*80)
        logger.info("")
        
        try:
            # 외래키 제약 비활성화
            cursor.execute("PRAGMA foreign_keys = OFF")
            
            # 테이블 드롭
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # 외래키 제약 재활성화
            cursor.execute("PRAGMA foreign_keys = ON")
            
            logger.info(f"✓ 테이블 '{table_name}' 드롭됨")
            
        except Exception as e:
            logger.error(f"✗ 테이블 '{table_name}' 드롭 실패: {str(e)}", exc_info=True)
            conn.close()
            return False
        
        logger.info("")
        
        # 변경사항 커밋
        conn.commit()
        
        # 관련 인덱스 확인 및 제거
        logger.info("="*80)
        logger.info("관련 인덱스 정리 중...")
        logger.info("="*80)
        logger.info("")
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        existing_indexes = [row[0] for row in cursor.fetchall()]
        
        # 테이블명이 포함된 인덱스 찾기
        related_indexes = [idx for idx in existing_indexes if table_name in idx.lower()]
        
        dropped_indexes = 0
        if related_indexes:
            for index_name in related_indexes:
                try:
                    cursor.execute(f"DROP INDEX IF EXISTS {index_name}")
                    logger.info(f"✓ 인덱스 '{index_name}' 드롭됨")
                    dropped_indexes += 1
                except Exception as e:
                    logger.error(f"✗ 인덱스 '{index_name}' 드롭 실패: {str(e)}", exc_info=True)
        else:
            logger.info(f"⊘ '{table_name}'과 관련된 인덱스가 없음 (스킵)")
        
        conn.commit()
        
        # 연결 종료
        conn.close()
        
        # 결과 요약
        logger.info("")
        logger.info("="*80)
        logger.info(f"✅ 완료!")
        logger.info(f"   - 드롭된 테이블: {table_name}")
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
    parser = argparse.ArgumentParser(
        description="지정된 테이블을 데이터베이스에서 DROP합니다",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  python drop_table.py job_queue
  python drop_table.py work_queue
  python drop_table.py tasks
        """
    )
    
    parser.add_argument(
        'table_name',
        type=str,
        help='DROP할 테이블의 이름'
    )
    
    args = parser.parse_args()
    
    try:
        success = drop_table(args.table_name)
        if not success:
            logger.error(f"테이블 '{args.table_name}' DROP 작업 중 오류가 발생했습니다")
            exit(1)
    except Exception as e:
        logger.error(f"메인 함수 실행 중 에러: {str(e)}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()

