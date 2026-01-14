from sv.backend.service.service_manager import get_service_manager
from sv.utils.logger import setup_logger, setup_common_logger

logger = setup_logger(__name__)

def initialize_logger() -> None:
    """로거 초기화"""
    setup_common_logger(None)

def clear_all_database_records() -> int:
    """
    JobQueue와 TaskQueue의 모든 기록을 삭제합니다.
    
    Returns:
        삭제된 레코드 총 개수
    """
    initialize_logger()
    
    # ServiceManager 초기화
    service_manager = get_service_manager()
    if not service_manager.is_initialized():
        logger.info("ServiceManager 초기화 중...")
        service_manager.initialize_all_services()
    
    # JobQueueService 획득
    job_queue_service = service_manager.get_job_queue_service()
    
    try:
        # 전체 작업 조회
        logger.info("="*80)
        logger.info("데이터베이스 모든 레코드 삭제 중...")
        logger.info("="*80)
        logger.info("")
        
        # 기존 작업 조회
        all_jobs = job_queue_service.get_all_jobs()
        logger.info(f"삭제할 Job 개수: {len(all_jobs)}")
        
        # 기존 작업 삭제
        logger.info("")
        logger.info(f"Job 테이블에서 {len(all_jobs)}개의 레코드 삭제 중...")
        deleted_jobs = 0
        for job in all_jobs:
            job_id = job['job_id']
            try:
                # Job 삭제
                success = job_queue_service.delete_job(job_id)
                if success:
                    deleted_jobs += 1
                    logger.info(f"✓ Job {job_id} 삭제됨")
                else:
                    logger.warning(f"✗ Job {job_id} 삭제 실패")
            except Exception as e:
                logger.error(f"✗ Job {job_id} 삭제 중 에러: {str(e)}", exc_info=True)
        
        logger.info(f"✓ 총 {deleted_jobs}개의 Job 삭제 완료")
        logger.info("")
        
        # 기존 작업의 태스크 조회
        all_tasks = job_queue_service.get_all_tasks()
        logger.info(f"삭제할 Task 개수: {len(all_tasks)}")
        
        # 기존 작업의 태스크 삭제
        logger.info("")
        logger.info(f"Task 테이블에서 {len(all_tasks)}개의 레코드 삭제 중...")
        deleted_tasks = 0
        for task in all_tasks:
            task_id = task['task_id']
            try:
                # Task 삭제
                success = job_queue_service.delete_task(task_id)
                if success:
                    deleted_tasks += 1
                    logger.info(f"✓ Task {task_id} 삭제됨")
                else:
                    logger.warning(f"✗ Task {task_id} 삭제 실패")
            except Exception as e:
                logger.error(f"✗ Task {task_id} 삭제 중 에러: {str(e)}", exc_info=True)
        
        logger.info(f"✓ 총 {deleted_tasks}개의 Task 삭제 완료")
        
        # 결과 요약
        total_deleted = deleted_jobs + deleted_tasks
        logger.info("")
        logger.info("="*80)
        logger.info(f"✅ 완료! 총 {total_deleted}개의 레코드가 삭제되었습니다")
        logger.info(f"   - Job: {deleted_jobs}개")
        logger.info(f"   - Task: {deleted_tasks}개")
        logger.info("="*80)
        
        return total_deleted
        
    except Exception as e:
        logger.error(f"❌ 에러 발생: {str(e)}", exc_info=True)
        return 0


def main():
    """메인 함수"""
    clear_all_database_records()


if __name__ == "__main__":
    main()
