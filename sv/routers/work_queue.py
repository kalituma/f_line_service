from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from sv.backend.service.service_manager import get_service_manager
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

router = APIRouter(prefix="/f_line_service", tags=["work-queue"])

# ==================== Request/Response Models ====================

class AddWorkRequest(BaseModel):
    """작업 추가 요청 모델"""
    frfr_info_id: str
    analysis_id: str

# ==================== API Endpoints ====================

@router.post("/add_works", response_model=dict)
async def add_work(request: AddWorkRequest):
    """
    새 작업을 추가합니다.
    
    - **frfr_id**: 산불 정보 ID
    - **analysis_id**: 분석 ID
    
    Returns:
        - work_id: 생성된 작업 ID (중복인 경우 None)
        - success: 성공 여부
    """
    try:
        service = get_service_manager().get_work_queue_service()
        
        work_id = service.add_work(request.frfr_id, request.analysis_id)
        
        if work_id is None:
            raise HTTPException(
                status_code=409,
                detail=f"Duplicate work: frfr_id={request.frfr_id}, analysis_id={request.analysis_id}"
            )
        
        logger.info(f"Work added via API: work_id={work_id}")
        return {"success": True, "work_id": work_id}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding work: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/works", response_model=dict)
async def get_all_works():
    """
    모든 작업 정보를 조회합니다.
    
    Returns:
        모든 작업 정보 (JSON 배열)
    """
    try:
        service = get_service_manager().get_work_queue_service()
        works = service.get_all_works()
        
        logger.info(f"Retrieved {len(works)} works via API")
        return {
            "success": True,
            "total": len(works),
            "works": works
        }
    
    except Exception as e:
        logger.error(f"Error getting all works: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/works/{work_id}", response_model=dict)
async def get_work_status(work_id: int):
    """
    작업 ID로 작업 정보를 조회합니다.

    - **work_id**: 작업 ID

    Returns:
        작업 정보 (없으면 404 에러)
    """
    try:
        service = get_service_manager().get_work_queue_service()
        work = service.get_work_by_id(work_id)

        if work is None:
            raise HTTPException(status_code=404, detail=f"Work not found: work_id={work_id}")

        return {"success": True, "work": work}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting work: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


