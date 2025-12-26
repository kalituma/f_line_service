import logging
from fastapi import APIRouter, HTTPException
from server.response_obj import WildfireRequest, WildfireResponse
from server.backend.service import get_wildfire_service

logger = logging.getLogger(__name__)

router = APIRouter()
service = get_wildfire_service()


@router.post("/sender")
async def wildfire_sender(request: WildfireRequest) -> WildfireResponse:
    """
    산불 정보를 조회하거나 데이터베이스에 저장합니다.

    Parameters:
        request (WildfireRequest): 산불 정보 ID와 분석 ID를 포함한 요청 본문

    Returns:
        WildfireResponse: 산불 위치, 관련 비디오 정보 등이 포함된 응답
    """
    logger.info(
        f"Wildfire request - frfr_info_id: {request.frfr_info_id}, "
        f"analysis_id: {request.analysis_id}"
    )

    if not request.frfr_info_id or not request.analysis_id:
        raise HTTPException(
            status_code=400,
            detail="frfr_info_id and analysis_id are required"
        )

    # 데이터베이스에서 조회
    response = service.get_wildfire(request.frfr_info_id)

    if response:
        logger.info(f"Found wildfire in database: {request.frfr_info_id}")
    else:
        logger.info(
            f"Wildfire not found in database, creating default response: "
            f"{request.frfr_info_id}"
        )
        # 데이터베이스에 없으면 기본값으로 생성
        response = service.create_wildfire(
            frfr_info_id=request.frfr_info_id,
            analysis_id=request.analysis_id,
            fire_location={"latitude": 36.30740755588261, "longitude": 128.45275734365313},
            videos=[
                {
                    "path": f"/data/helivid/hv_proc/{request.frfr_info_id}/FPA601/FPA630_20251127_190145_7366_000.mkv",
                    "name": "FPA630_20251127_190145_7366_000.mkv",
                    "add_time": "2025-11-27 10:03:47.550",
                    "analysis_status": "STAT_001",
                },
                {
                    "path": f"/data/helivid/hv_proc/{request.frfr_info_id}/FPA601/FPA630_20251127_190318_1478_000.mkv",
                    "name": "FPA630_20251127_190318_1478_000.mkv",
                    "add_time": "2025-11-27 10:03:58.703",
                    "analysis_status": "STAT_001",
                },
            ],
        )

    logger.info(f"Returning wildfire response for {request.frfr_info_id}")
    return response


@router.get("/sender/{frfr_info_id}")
async def get_wildfire(frfr_info_id: str) -> WildfireResponse:
    """
    저장된 산불 정보를 조회합니다.

    Parameters:
        frfr_info_id (str): 산불 정보 ID

    Returns:
        WildfireResponse: 산불 정보
    """
    logger.info(f"Getting wildfire: {frfr_info_id}")

    response = service.get_wildfire(frfr_info_id)
    if not response:
        raise HTTPException(status_code=404, detail="Wildfire not found")

    return response


@router.get("/sender")
async def list_wildfires() -> dict:
    """
    모든 산불 정보를 조회합니다.

    Returns:
        dict: 산불 정보 리스트
    """
    logger.info("Listing all wildfires")

    wildfires = service.get_all_wildfires()
    return {"count": len(wildfires), "data": wildfires}