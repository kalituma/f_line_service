from fastapi import APIRouter, HTTPException, status
from server.backend.service import (
    get_wildfire_service,
    AnalysisCreate,
    AnalysisResponse,
)
from typing import List

router = APIRouter(prefix="/api/analysis", tags=["analysis"])


@router.post("/analyses", response_model=AnalysisResponse)
async def create_analysis(analysis: AnalysisCreate) -> AnalysisResponse:
    """
    Create a new analysis record.

    Args:
        analysis: Analysis data to create

    Returns:
        Created analysis response
    """
    service = get_wildfire_service()
    result = service.create_analysis(
        analysis_id=analysis.analysis_id,
        frfr_info_id=analysis.frfr_info_id,
        video_updates=analysis.video_updates,
    )
    return result


@router.get("/analyses/{analysis_id}/{frfr_info_id}", response_model=AnalysisResponse)
async def get_analysis(
    analysis_id: str, frfr_info_id: str
) -> AnalysisResponse:
    """
    Get analysis by ID (using: analysis_id + frfr_info_id).

    Args:
        analysis_id: Analysis ID
        frfr_info_id: Wildfire info ID

    Returns:
        Analysis response
    """
    service = get_wildfire_service()
    result = service.get_analysis(analysis_id, frfr_info_id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analysis not found: {analysis_id}/{frfr_info_id}",
        )
    return result


@router.get("/analyses/by-analysis/{analysis_id}", response_model=List[AnalysisResponse])
async def get_analyses_by_analysis_id(
    analysis_id: str,
) -> List[AnalysisResponse]:
    """
    Get all analyses by analysis_id.

    Args:
        analysis_id: Analysis ID

    Returns:
        List of analysis responses
    """
    service = get_wildfire_service()
    return service.get_analyses_by_analysis_id(analysis_id)


@router.get("/analyses/by-wildfire/{frfr_info_id}", response_model=List[AnalysisResponse])
async def get_analyses_by_frfr_id(
    frfr_info_id: str,
) -> List[AnalysisResponse]:
    """
    Get all analyses by frfr_info_id.

    Args:
        frfr_info_id: Wildfire info ID

    Returns:
        List of analysis responses
    """
    service = get_wildfire_service()
    return service.get_analyses_by_frfr_id(frfr_info_id)


@router.get("/analyses", response_model=List[AnalysisResponse])
async def get_all_analyses() -> List[AnalysisResponse]:
    """
    Get all analyses.

    Returns:
        List of all analysis responses
    """
    service = get_wildfire_service()
    return service.get_all_analyses()


@router.put("/analyses/{analysis_id}/{frfr_info_id}", response_model=AnalysisResponse)
async def update_analysis(
    analysis_id: str,
    frfr_info_id: str,
    analysis: AnalysisCreate,
) -> AnalysisResponse:
    """
    Update an analysis record.

    Args:
        analysis_id: Analysis ID
        frfr_info_id: Wildfire info ID
        analysis: Updated analysis data

    Returns:
        Updated analysis response
    """
    service = get_wildfire_service()
    result = service.update_analysis(
        analysis_id=analysis_id,
        frfr_info_id=frfr_info_id,
        video_updates=analysis.video_updates,
    )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analysis not found: {analysis_id}/{frfr_info_id}",
        )
    return result


@router.delete("/analyses/{analysis_id}/{frfr_info_id}")
async def delete_analysis(
    analysis_id: str, frfr_info_id: str
) -> dict:
    """
    Delete an analysis record.

    Args:
        analysis_id: Analysis ID
        frfr_info_id: Wildfire info ID

    Returns:
        Success response
    """
    service = get_wildfire_service()
    success = service.delete_analysis(analysis_id, frfr_info_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analysis not found: {analysis_id}/{frfr_info_id}",
        )
    return {"success": True, "message": "Analysis deleted successfully"}


@router.delete("/analyses/by-analysis/{analysis_id}")
async def delete_analyses_by_analysis_id(analysis_id: str) -> dict:
    """
    Delete all analyses by analysis_id.

    Args:
        analysis_id: Analysis ID

    Returns:
        Success response
    """
    service = get_wildfire_service()
    success = service.delete_analyses_by_analysis_id(analysis_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No analyses found for analysis_id: {analysis_id}",
        )
    return {"success": True, "message": "Analyses deleted successfully"}


@router.delete("/analyses/by-wildfire/{frfr_info_id}")
async def delete_analyses_by_frfr_id(frfr_info_id: str) -> dict:
    """
    Delete all analyses by frfr_info_id.

    Args:
        frfr_info_id: Wildfire info ID

    Returns:
        Success response
    """
    service = get_wildfire_service()
    success = service.delete_analyses_by_frfr_id(frfr_info_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No analyses found for frfr_info_id: {frfr_info_id}",
        )
    return {"success": True, "message": "Analyses deleted successfully"}


@router.post("/analyses/{analysis_id}/{frfr_info_id}/video-updates")
async def add_video_update(
    analysis_id: str,
    frfr_info_id: str,
    video_name: str,
    analysis_status: str,
) -> AnalysisResponse:
    """
    Add video update to analysis.

    Args:
        analysis_id: Analysis ID
        frfr_info_id: Wildfire info ID
        video_name: Video name
        analysis_status: Analysis status

    Returns:
        Updated analysis response
    """
    service = get_wildfire_service()
    result = service.add_video_update_to_analysis(
        analysis_id=analysis_id,
        frfr_info_id=frfr_info_id,
        video_name=video_name,
        analysis_status=analysis_status,
    )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analysis not found: {analysis_id}/{frfr_info_id}",
        )
    return result


@router.delete("/analyses/{analysis_id}/{frfr_info_id}/video-updates/{video_name}")
async def remove_video_update(
    analysis_id: str,
    frfr_info_id: str,
    video_name: str,
) -> AnalysisResponse:
    """
    Remove video update from analysis.

    Args:
        analysis_id: Analysis ID
        frfr_info_id: Wildfire info ID
        video_name: Video name to remove

    Returns:
        Updated analysis response
    """
    service = get_wildfire_service()
    result = service.remove_video_update_from_analysis(
        analysis_id=analysis_id,
        frfr_info_id=frfr_info_id,
        video_name=video_name,
    )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analysis not found: {analysis_id}/{frfr_info_id}",
        )
    return result
