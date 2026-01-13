from typing import Dict, Any, List

from sv.daemon.module.http_request_client import post_json, HttpRequestError
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)


def send_video_status_update(
    update_url: str,
    frfr_id: int,
    analysis_id: int,
    video_updates: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    비디오 상태 업데이트를 서버로 전송
    
    Args:
        update_url: 업데이트 요청을 보낼 URL
        frfr_id: FRFR 정보 ID
        analysis_id: 분석 ID
        video_updates: 비디오 업데이트 리스트 (각 항목은 video_name, analysis_status 포함)
            예: [
                {
                    "video_name": "video_001.mp4",
                    "analysis_status": "STAT_002"
                },
                ...
            ]
    
    Returns:
        서버 응답
    """
    try:
        request_data = {
            "frfr_info_id": frfr_id,
            "analysis_id": analysis_id,
            "video_updates": video_updates
        }
        
        logger.info(f"📋 업데이트 요청 데이터: {request_data}")
        
        response = post_json(
            url=update_url,
            json_data=request_data,
            timeout=30,
            verify_ssl=False
        )
        
        logger.info(f"✅ 데이터 업데이트 성공: {response}")
        return response
        
    except HttpRequestError as e:
        logger.error(f"❌ 데이터 업데이트 요청 실패: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"❌ 업데이트 중 예상치 못한 에러: {str(e)}", exc_info=True)
        raise
