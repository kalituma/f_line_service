from sv.backend.service.service_manager import get_service_manager
from sv.backend.job_status import JobStatus
from sv.utils.logger import setup_logger, setup_common_logger

logger = setup_logger(__name__)

def initialize_logger() -> None:
    """로거 초기화"""
    setup_common_logger(None)

def reset_processing_jobs_to_pending() -> int:
    """
    'processing' 상태인 Job들을 'pending'으로 변경
    
    Returns:
        변경된 job 개수
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
        # 'processing' 상태인 모든 jobs 조회
        logger.info("='*80")
        logger.info("Processing 상태인 Jobs 조회 중...")
        logger.info("="*80)
        
        processing_jobs = job_queue_service.get_jobs_by_status(JobStatus.PROCESSING)
        
        if not processing_jobs:
            logger.info("✓ Processing 상태인 Job이 없습니다")
            return 0
        
        logger.info(f"✓ Processing 상태인 Job {len(processing_jobs)}개 발견")
        logger.info("")
        
        # 각 job을 'pending'으로 변경
        logger.info("="*80)
        logger.info("Job 상태 변경 중...")
        logger.info("="*80)
        
        updated_count = 0
        for job in processing_jobs:
            job_id = job['job_id']
            frfr_id = job['frfr_id']
            analysis_id = job['analysis_id']
            
            try:
                # 상태를 'pending'으로 변경
                success = job_queue_service.update_job_status(job_id, JobStatus.PENDING)
                
                if success:
                    logger.info(f"✓ Job {job_id} (frfr_id={frfr_id}, analysis_id={analysis_id}) "
                              f"변경됨: processing → pending")
                    updated_count += 1
                else:
                    logger.warning(f"✗ Job {job_id} 변경 실패")
                    
            except Exception as e:
                logger.error(f"✗ Job {job_id} 변경 중 에러: {str(e)}", exc_info=True)
        
        # 결과 요약
        logger.info("")
        logger.info("="*80)
        logger.info(f"✅ 완료! 총 {updated_count}개의 Job이 pending으로 변경되었습니다")
        logger.info("="*80)
        
        return updated_count
        
    except Exception as e:
        logger.error(f"❌ 에러 발생: {str(e)}", exc_info=True)
        return 0


def main():
    """메인 함수"""
    reset_processing_jobs_to_pending()


if __name__ == "__main__":
    main()
