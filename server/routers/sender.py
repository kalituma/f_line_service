import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from server.response_obj import (
    WildfireVideoRequest,
    WildfireInfoListResponse,
    WildfireInfoPair,
)
from server.backend.service.query_video_service import QueryVideoService, QueryVideoResponse
from server.backend.service.fire_info_service import FireInfoService
from server.backend.service.analysis_status_service import AnalysisStatusService


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/wildfire-data-sender/api/wildfire", tags=["wildfire"])
service = QueryVideoService()
fire_info_service = FireInfoService()
analysis_status_service = AnalysisStatusService()

@router.get(
    "/sender",
    response_model=WildfireInfoListResponse,
    responses={
        500: {"description": "서버 내부 오류"},
    }
)
async def get_wildfire_info_list() -> WildfireInfoListResponse:
    """
    저장된 모든 산불 정보 및 분석 ID를 조회합니다.
    frfr_info_id를 int로 파싱했을 때 큰 순서대로 반환합니다.

    Returns:
        WildfireInfoListResponse: 저장된 산불 정보 목록 (frfr_info_id 큰 순서 정렬)
    """
    try:
        logger.info("Getting all wildfire information list")

        # 1. 모든 산불 정보 조회
        all_fire_locations = fire_info_service.get_all_fire_locations()
        if not all_fire_locations:
            logger.info("No wildfire data found")
            return WildfireInfoListResponse(total_count=0, data=[])

        # 2. 모든 분석 상태 정보 조회
        all_analysis_statuses = analysis_status_service.get_all_analysis_status()
        if not all_analysis_statuses:
            logger.info("No analysis status data found")
            return WildfireInfoListResponse(total_count=0, data=[])

        # 3. frfr_info_id와 analysis_id 쌍을 unique하게 추출
        unique_pairs = {}
        for analysis_status in all_analysis_statuses:
            frfr_info_id = analysis_status.get("frfr_info_id")
            analysis_id = analysis_status.get("analysis_id")
            
            if frfr_info_id and analysis_id:
                # frfr_info_id를 key로 사용하여 unique하게 저장
                # 같은 frfr_info_id에 대해 첫 번째 analysis_id만 저장
                if frfr_info_id not in unique_pairs:
                    unique_pairs[frfr_info_id] = analysis_id

        # 4. frfr_info_id를 int로 파싱하여 큰 순서대로 정렬
        sorted_pairs = []
        for frfr_info_id, analysis_id in unique_pairs.items():
            sorted_pairs.append({
                "frfr_info_id": frfr_info_id,
                "analysis_id": analysis_id,
            })
            

        # 5. frfr_info_id_int를 기준으로 내림차순(큰 순서)으로 정렬
        sorted_pairs.sort(key=lambda x: int(x["frfr_info_id"]), reverse=True)

        # 6. WildfireInfoPair 객체 생성
        data = [WildfireInfoPair(**pair) for pair in sorted_pairs]

        logger.info(
            f"Successfully retrieved wildfire information list: "
            f"{len(data)} unique frfr_info_id found"
        )
        
        return WildfireInfoListResponse(total_count=len(data), data=data)

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "code": "LIST_500",
                "message": "서버 내부 오류가 발생했습니다",
            }
        )


@router.post(
    "/sender",
    response_model=QueryVideoResponse,
    responses={
        400: {"description": "잘못된 요청 형식"},
        404: {"description": "산불 또는 분석 데이터를 찾을 수 없음"},
        500: {"description": "서버 내부 오류"},
    }
)
async def get_wildfire_video(video_request: WildfireVideoRequest) -> QueryVideoResponse:
    """
    저장된 산불 정보 및 비디오 정보를 조회합니다.

    Parameters:
        video_request (WildfireVideoRequest): 
            - frfr_info_id (str): 산불 정보 ID
            - analysis_id (str): 분석 ID

    Returns:
        WildfireResponse: 산불 위치 정보와 비디오 목록
    """
    try:
        logger.info(
            f"Getting wildfire video data: frfr_info_id={video_request.frfr_info_id}, "
            f"analysis_id={video_request.analysis_id}"
        )

        # 입력값 검증
        if not video_request.frfr_info_id or not video_request.analysis_id:
            logger.warning(f"Invalid request parameters: {video_request}")
            return JSONResponse(
                status_code=400,
                content={
                    "code": "SEND_400",
                    "message": "필수 필드가 누락되었습니다",
                    "frfr_info_id": 'null',
                    "analysis_id": 'null'
                }
            )

        # QueryVideoService를 사용하여 통합 데이터 조회
        logger.info("[DEBUG] Calling QueryVideoService.get_query_data_as_object...")
        response = service.get_query_data_as_object(
            frfr_info_id=video_request.frfr_info_id,
            analysis_id=video_request.analysis_id
        )

        if not response:
            logger.warning(
                f"Data not found for frfr_info_id={video_request.frfr_info_id}, "
                f"analysis_id={video_request.analysis_id}"
            )
            return JSONResponse(
                status_code=404,
                content={
                    "code": "SEND_404",
                    "message": "해당 산불 정보를 찾을 수 없습니다",
                    "frfr_info_id": video_request.frfr_info_id,
                    "analysis_id": video_request.analysis_id
                }
            )

        logger.info(
            f"Successfully retrieved wildfire data: "
            f"{len(response.videos)} videos found"
        )
        return response

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