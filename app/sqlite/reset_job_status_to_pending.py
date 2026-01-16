from ray import worker

from sv.backend.service.service_manager import get_service_manager
from sv.backend.work_status import WorkStatus
from sv.utils.logger import setup_logger, setup_common_logger

logger = setup_logger(__name__)

def initialize_logger() -> None:
    """로거 초기화"""
    setup_common_logger(None)

def reset_processing_works_to_pending() -> int:
    """
    'processing' 상태인 work들을 'pending'으로 변경
    
    Returns:
        변경된 work 개수
    """
    initialize_logger()
    
    # ServiceManager 초기화
    service_manager = get_service_manager()
    if not service_manager.is_initialized():
        logger.info("ServiceManager 초기화 중...")
        service_manager.initialize_all_services()
    
    # workQueueService 획득
    work_queue_service = service_manager.get_work_queue_service()
    
    try:
        # 'processing' 상태인 모든 works 조회
        logger.info("='*80")
        logger.info("Processing 상태인 works 조회 중...")
        logger.info("="*80)
        
        processing_works = work_queue_service.get_works_by_status(WorkStatus.PROCESSING)
        processing_works+=work_queue_service.get_works_by_status(WorkStatus.COMPLETED)
        processing_works+= work_queue_service.get_works_by_status(WorkStatus.FAILED)
        
        if not processing_works:
            logger.info("✓ Processing 상태인 work이 없습니다")
            return 0
        
        logger.info(f"✓ Processing 상태인 work {len(processing_works)}개 발견")
        logger.info("")
        
        # 각 work을 'pending'으로 변경
        logger.info("="*80)
        logger.info("work 상태 변경 중...")
        logger.info("="*80)
        
        updated_count = 0
        for work in processing_works:
            work_id = work['work_id']
            frfr_id = work['frfr_id']
            analysis_id = work['analysis_id']
            
            try:
                # 상태를 'pending'으로 변경
                success = work_queue_service.update_work_status(work_id, WorkStatus.PENDING)
                
                if success:
                    logger.info(f"✓ work {work_id} (frfr_id={frfr_id}, analysis_id={analysis_id}) "
                              f"변경됨: processing → pending")
                    updated_count += 1
                else:
                    logger.warning(f"✗ work {work_id} 변경 실패")
                    
            except Exception as e:
                logger.error(f"✗ work {work_id} 변경 중 에러: {str(e)}", exc_info=True)
        
        # 결과 요약
        logger.info("")
        logger.info("="*80)
        logger.info(f"✅ 완료! 총 {updated_count}개의 work이 pending으로 변경되었습니다")
        logger.info("="*80)
        
        return updated_count
        
    except Exception as e:
        logger.error(f"❌ 에러 발생: {str(e)}", exc_info=True)
        return 0


def main():
    """메인 함수"""
    reset_processing_works_to_pending()


if __name__ == "__main__":
    main()
