import logging
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

from server.backend.service.analyzed_data_service import AnalyzedDataService
from server.backend.service.fire_info_service import FireInfoService
from server.backend.service.analysis_status_service import AnalysisStatusService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/wildfire-data-receiver/api/wildfire", tags=["analyzed-data"])
analyzed_data_service = AnalyzedDataService()
fire_info_service = FireInfoService()
analysis_status_service = AnalysisStatusService()


# ==================== 요청 모델 ====================
class GeoJSONProperties(BaseModel):
    """GeoJSON Feature Properties"""
    remarks: Optional[str] = None
    fire_status: Optional[str] = None


class GeoJSONGeometry(BaseModel):
    """GeoJSON Geometry"""
    type: str  # e.g., "MultiLineString", "Point", "Polygon"
    coordinates: List[Any]


class GeoJSONFeature(BaseModel):
    """GeoJSON Feature"""
    type: str = "Feature"
    geometry: GeoJSONGeometry
    properties: GeoJSONProperties


class CRSProperties(BaseModel):
    """CRS Properties"""
    name: str


class CRS(BaseModel):
    """Coordinate Reference System"""
    type: str = "name"
    properties: CRSProperties


class AnalyzedDataRequest(BaseModel):
    """분석 결과 데이터 요청"""
    type: str = "FeatureCollection"
    frfr_info_id: str
    analysis_id: str
    timestamp: str
    crs: CRS
    features: List[GeoJSONFeature]

    @field_validator('frfr_info_id', mode='before')
    @classmethod
    def convert_frfr_info_id_to_str(cls, v):
        """frfr_info_id를 문자열로 변환"""
        return str(v)


# ==================== 응답 모델 ====================
class AnalyzedDataResponse(BaseModel):
    """분석 결과 데이터 저장 성공 응답"""
    code: str
    message: str
    frfr_info_id: str    


# ==================== 헬퍼 함수 ====================
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
            "code": "RECV_404",
            "message": "해당 산불 정보를 찾을 수 없습니다",
            "frfr_info_id": frfr_info_id
        }
    )


def _bad_request_response(message: str) -> JSONResponse:
    """
    400 Bad Request 응답을 생성합니다.
    
    Args:
        message: 에러 메시지
        
    Returns:
        JSONResponse: 400 응답
    """
    return JSONResponse(
        status_code=400,
        content={
            "code": "RECV_400",
            "message": message,
            "frfr_info_id": "null"
        }
    )


# ==================== API 엔드포인트 ====================
@router.post(
    "/data",
    responses={
        200: {"description": "성공"},
        400: {"description": "잘못된 요청 형식"},
        404: {"description": "산불 또는 분석 데이터를 찾을 수 없음"},
        500: {"description": "서버 내부 오류"},
    }
)
async def save_analyzed_data(data_request: AnalyzedDataRequest):
    """    
    분석 결과 데이터를 저장합니다 (insert or update).

    Parameters:
        data_request (AnalyzedDataRequest): 
            - type (str): "FeatureCollection"
            - frfr_info_id (str): 산불 정보 ID
            - analysis_id (str): 분석 ID
            - timestamp (str, optional): 타임스탬프
            - crs (object, optional): 좌표계 정보
            - features (list): GeoJSON Feature 리스트

    Returns:
        JSONResponse: save result
    """
    try:
        logger.info(
            f"Saving analyzed data: frfr_info_id={data_request.frfr_info_id}, "
            f"analysis_id={data_request.analysis_id}, "
            f"features_count={len(data_request.features)}"
        )

        # 입력값 검증
        if not data_request.frfr_info_id or not data_request.analysis_id or not data_request.features or len(data_request.features) == 0:
            logger.warning(f"Invalid request parameters: missing frfr_info_id or analysis_id")
            return _bad_request_response("잘못된 요청입니다.")

        # 산불 정보 존재 여부 확인
        logger.info(f"[DEBUG] Checking fire info existence for {data_request.frfr_info_id}...")
        fire_info_exists = fire_info_service.check_fire_location_exists(data_request.frfr_info_id)
        
        if not fire_info_exists:
            logger.warning(f"Fire location not found for {data_request.frfr_info_id}")
            return _not_found_response(data_request.frfr_info_id, data_request.analysis_id)

        # 분석 그룹 존재 여부 확인
        logger.info(f"[DEBUG] Checking analysis status existence for {data_request.analysis_id}...")
        analysis_status_exists = analysis_status_service.check_analysis_status_exists(
            analysis_id=data_request.analysis_id
        )

        if not analysis_status_exists:
            logger.warning(f"Analysis status not found for {data_request.analysis_id}")
            return _not_found_response(data_request.frfr_info_id, data_request.analysis_id)

        # AnalyzedDataService를 사용하여 저장/업데이트
        logger.info("[DEBUG] Calling AnalyzedDataService.save_analyzed_data...")
        
        request_data = {
            "type": data_request.type,
            "frfr_info_id": data_request.frfr_info_id,
            "analysis_id": data_request.analysis_id,
            "timestamp": data_request.timestamp,
            "crs": data_request.crs.model_dump() if data_request.crs else None,
            "features": [feature.model_dump() for feature in data_request.features],
        }
        
        result = analyzed_data_service.save_analyzed_data(request_data)

        if not result.get("success"):
            logger.error(
                f"Failed to save analyzed data: {result.get('message')}"
            )
            return JSONResponse(
                status_code=500,
                content={
                    "code": "RECV_500",
                    "message": result.get("message", "분석 데이터 저장 실패"),
                    "frfr_info_id": data_request.frfr_info_id
                }
            )

        logger.info(
            f"Successfully saved analyzed data: "
            f"operation={result.get('operation')}, "
            f"record_id={result.get('record_id')}"
        )
        
        # 성공 응답 - JSONResponse로 직접 반환
        return JSONResponse(
            status_code=200,
            content={
                "code": "RECV_200",
                "message": "데이터 수신 완료",
                "frfr_info_id": data_request.frfr_info_id,
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
                "code": "RECV_500",
                "message": "서버 내부 오류가 발생했습니다",
                "frfr_info_id": data_request.frfr_info_id if hasattr(data_request, 'frfr_info_id') else "null"
            }
        )