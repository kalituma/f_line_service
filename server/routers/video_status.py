import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from server.response_obj import (
    VideoStatusUpdateRequest,
)
from server.backend.service.analysis_status_service import AnalysisStatusService
from server.backend.service.fire_info_service import FireInfoService
from server.backend.service.query_video_service import QueryVideoService


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/wildfire-data-receiver/api/wildfire", tags=["video-status"])
analysis_status_service = AnalysisStatusService()
fire_info_service = FireInfoService()
query_video_service = QueryVideoService()


def _not_found_response(frfr_info_id: str, analysis_id: str) -> JSONResponse:
    """
    404 Not Found 응답을 생성합니다.
    
    Args:
        frfr_info_id: 산불 정보 ID
        analysis_id: 분석 ID
        
    Returns:
        JSONResponse: 404 응답
    """
    return JSONResponse(
        status_code=404,
        content={
            "code": "STUP_404",
            "message": "해당 분석 그룹을 찾을 수 없습니다",
            "frfr_info_id": frfr_info_id,
            "analysis_id": analysis_id
        }
    )


@router.post(
    "/video-status",
    responses={
        200: {"description": "성공"},
        400: {"description": "잘못된 요청 형식"},
        404: {"description": "산불 또는 분석 데이터를 찾을 수 없음"},
        500: {"description": "서버 내부 오류"},
    }
)
async def update_video_status(video_request: VideoStatusUpdateRequest):
    """    
    insert or update video status

    Parameters:
        video_request (VideoStatusUpdateRequest): 
            - frfr_info_id (str): frfr info id
            - analysis_id (str): analysis id
            - video_updates (List[VideoStatusUpdate]): video status update list

    Returns:
        VideoStatusResponse: update result
    """
    try:
        logger.info(
            f"Updating video status: frfr_info_id={video_request.frfr_info_id}, "
            f"analysis_id={video_request.analysis_id}, "
            f"updates_count={len(video_request.video_updates)}"
        )

        # 입력값 검증
        if not video_request.frfr_info_id or not video_request.analysis_id or not video_request.video_updates or len(video_request.video_updates) == 0:
            logger.warning(f"Invalid request parameters: {video_request}")
            return JSONResponse(
                status_code=400,
                content={
                    "code": "STUP_400",
                    "message": "필수 필드가 누락되었습니다",
                    "frfr_info_id": 'null',
                    "analysis_id": 'null'
                }
            )

        # 데이터 존재 여부 확인 - QueryVideoService 활용
        logger.info("[DEBUG] Checking data existence using QueryVideoService...")
        check_result = query_video_service.check_video_exists(
            frfr_info_id=video_request.frfr_info_id,
            video_updates=video_request.video_updates
        )

        # 산불 정보 미존재
        if not check_result["frfr_info_exists"]:
            logger.warning(f"Fire location not found for {video_request.frfr_info_id}")
            return _not_found_response(video_request.frfr_info_id, video_request.analysis_id)

        # 분석 그룹 미존재
        logger.info("[DEBUG] Checking analysis status existence for analysis_id...")
        analysis_status_exists = analysis_status_service.check_analysis_status_exists(
            analysis_id=video_request.analysis_id
        )

        if not analysis_status_exists:
            logger.warning(f"Analysis status not found for {video_request.analysis_id}")
            return _not_found_response(video_request.frfr_info_id, video_request.analysis_id)

        if not check_result["all_exist"]:
            missing_videos = [v["video_name"] for v in check_result["videos"] if not v["exists"]]
            logger.warning(
                f"Some videos not found for frfr_info_id={video_request.frfr_info_id}: "
                f"{missing_videos}"
            )
            return _not_found_response(video_request.frfr_info_id, video_request.analysis_id)

        # AnalysisStatusService를 사용하여 배치 저장/업데이트
        logger.info("[DEBUG] Calling AnalysisStatusService.save_analysis_status_batch...")
        
        request_data = {
            "frfr_info_id": video_request.frfr_info_id,
            "analysis_id": video_request.analysis_id,
            "video_updates": [
                {
                    "video_name": update.video_name,
                    "analysis_status": update.analysis_status
                }
                for update in video_request.video_updates
            ]
        }
        
        result = analysis_status_service.save_analysis_status_batch(request_data)

        logger.info(
            f"Successfully updated video status: "
            f"saved={result.get('saved_count', 0)}, failed={result.get('failed_count', 0)}"
        )
        
        # 성공 응답 - JSONResponse로 직접 반환
        return JSONResponse(
            status_code=200,
            content={
                "code": "STUP_200",
                "message": "영상 상태 업데이트 완료",
                "frfr_info_id": video_request.frfr_info_id,
                "analysis_id": video_request.analysis_id
            }
        )

    except HTTPException:
        # HTTPException은 그대로 re-raise
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "code": "SEND_500",
                "message": "서버 내부 오류가 발생했습니다",
                "frfr_info_id": video_request.frfr_info_id,
                "analysis_id": video_request.analysis_id
            }
        )
