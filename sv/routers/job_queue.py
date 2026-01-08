from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from sv.backend.service.job_queue_service import JobQueueService
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

router = APIRouter(prefix="/f_line_service/jobs", tags=["job-queue"])

# ==================== Request/Response Models ====================

class AddJobRequest(BaseModel):
    """작업 추가 요청 모델"""
    frfr_info_id: str
    analysis_id: str

# ==================== API Endpoints ====================

@router.post("/add_jobs", response_model=dict)
async def add_job(request: AddJobRequest):
    """
    새 작업을 추가합니다.
    
    - **frfr_info_id**: 산불 정보 ID
    - **analysis_id**: 분석 ID
    
    Returns:
        - job_id: 생성된 작업 ID (중복인 경우 None)
        - success: 성공 여부
    """
    try:
        service = JobQueueService()
        job_id = service.add_job(request.frfr_info_id, request.analysis_id)
        
        if job_id is None:
            raise HTTPException(
                status_code=409,
                detail=f"Duplicate job: frfr_info_id={request.frfr_info_id}, analysis_id={request.analysis_id}"
            )
        
        logger.info(f"Job added via API: job_id={job_id}")
        return {"success": True, "job_id": job_id}
    
    except Exception as e:
        logger.error(f"Error adding job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{job_id}", response_model=dict)
async def get_job_status(job_id: int):
    """
    작업 ID로 작업 정보를 조회합니다.

    - **job_id**: 작업 ID

    Returns:
        작업 정보 (없으면 404 에러)
    """
    try:
        service = JobQueueService()
        job = service.get_job_by_id(job_id)

        if job is None:
            raise HTTPException(status_code=404, detail=f"Job not found: job_id={job_id}")

        return {"success": True, "job": job}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/jobs", response_model=dict)
async def get_all_jobs():
    """
    모든 작업 정보를 조회합니다.
    
    Returns:
        모든 작업 정보 (JSON 배열)
    """
    try:
        service = JobQueueService()
        jobs = service.get_all_jobs()
        
        logger.info(f"Retrieved {len(jobs)} jobs via API")
        return {
            "success": True,
            "total": len(jobs),
            "jobs": jobs
        }
    
    except Exception as e:
        logger.error(f"Error getting all jobs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/tasks", response_model=dict)
async def get_all_tasks():
    """
    모든 작업 태스크 정보를 조회합니다.
    
    Returns:
        모든 태스크 정보 (JSON 배열)
    """
    try:
        service = JobQueueService()
        tasks = service.get_all_tasks()
        
        logger.info(f"Retrieved {len(tasks)} tasks via API")
        return {
            "success": True,
            "total": len(tasks),
            "tasks": tasks
        }
    
    except Exception as e:
        logger.error(f"Error getting all tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
