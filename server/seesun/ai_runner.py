"""
AI 분석 시작 함수를 연결하기 위한 어댑터.
실제 AI 모듈의 분석 시작 로직으로 교체해 사용한다.
"""

from loguru import logger
from typing import Optional

from sv.backend.service.service_manager import get_service_manager

def add_job(frfr_id: str, analysis_id: str) -> Optional[int]:
    """
    새로운 Job 추가 (Main Loop에서 호출)

    Args:
        frfr_id: 산불 정보 ID
        analysis_id: 분석 ID

    Returns:
        생성된 job_id 또는 None (중복 또는 에러)
    """
    try:
        service_manager = get_service_manager()
        if not service_manager.is_initialized():
            logger.info("ServiceManager 초기화 중...")
            service_manager.initialize_all_services()

        job_id = get_service_manager().get_job_queue_service().add_job(frfr_id, analysis_id)
        if job_id:
            logger.info(f"Job added: job_id={job_id}, frfr_id={frfr_id}")
        else:
            logger.warning(f"Job already exists: frfr_id={frfr_id}")
        return job_id
    except Exception as e:
        logger.error(f"Error adding job: {str(e)}", exc_info=True)
        return None

def start_analysis(event_cd: str, analysis_cd: str):
    """
    AI 분석을 시작한다.
    필요한 경우 이 함수를 실제 AI 진입점으로 대체하거나 진입점 관련 함수로 교체
    """
    logger.info("AI 분석 시작 요청: event_cd={} analysis_cd={}", event_cd, analysis_cd)
    add_job(frfr_id=event_cd, analysis_id=analysis_cd)


