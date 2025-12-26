from fastapi import APIRouter, HTTPException, Path, Query
from typing import List
import logging
from server.backend.service import get_wildfire_service
from server.backend.service import WildfireVideoResponse, WildfireVideoCreate
from server.backend.service.video_service import WildfireVideoService

logger = logging.getLogger(__name__)

router = APIRouter()
service = get_wildfire_service()
video_service = WildfireVideoService()


@router.post("/videos")
async def create_video(video: WildfireVideoCreate) -> WildfireVideoResponse:
    """
    새로운 산불 비디오 정보를 저장합니다.

    Parameters:
        video (WildfireVideoCreate): 비디오 정보 (frfr_info_id, video_name, video_type, video_path)

    Returns:
        WildfireVideoResponse: 저장된 비디오 정보
    """
    logger.info(
        f"Creating video - frfr_info_id: {video.frfr_info_id}, "
        f"video_name: {video.video_name}"
    )

    response = service.create_wildfire_video(
        frfr_info_id=video.frfr_info_id,
        video_name=video.video_name,
        video_type=video.video_type,
        video_path=video.video_path,
    )

    return response


@router.get("/videos/{frfr_info_id}/{video_name}")
async def get_video(
    frfr_info_id: str = Path(..., description="산불 정보 ID"),
    video_name: str = Path(..., description="비디오 이름"),
) -> WildfireVideoResponse:
    """
    frfr_info_id와 video_name으로 비디오 정보를 조회합니다.

    Parameters:
        frfr_info_id (str): 산불 정보 ID
        video_name (str): 비디오 이름

    Returns:
        WildfireVideoResponse: 비디오 정보
    """
    logger.info(
        f"Getting video - frfr_info_id: {frfr_info_id}, video_name: {video_name}"
    )

    response = service.get_wildfire_video(frfr_info_id, video_name)
    if not response:
        raise HTTPException(
            status_code=404,
            detail=f"Video not found: {frfr_info_id}/{video_name}"
        )

    return response


@router.get("/videos/{frfr_info_id}")
async def list_videos(
    frfr_info_id: str = Path(..., description="산불 정보 ID"),
) -> dict:
    """
    특정 frfr_info_id의 모든 비디오를 조회합니다.

    Parameters:
        frfr_info_id (str): 산불 정보 ID

    Returns:
        dict: 비디오 목록
    """
    logger.info(f"Listing videos for frfr_info_id: {frfr_info_id}")

    videos = service.get_wildfire_videos(frfr_info_id)
    return {"count": len(videos), "data": videos}


@router.put("/videos/{frfr_info_id}/{video_name}")
async def update_video(
    frfr_info_id: str = Path(..., description="산불 정보 ID"),
    video_name: str = Path(..., description="비디오 이름"),
    video_type: str = None,
    video_path: str = None,
) -> WildfireVideoResponse:
    """
    비디오 정보를 업데이트합니다.

    Parameters:
        frfr_info_id (str): 산불 정보 ID
        video_name (str): 비디오 이름
        video_type (str, optional): 업데이트할 비디오 타입
        video_path (str, optional): 업데이트할 비디오 경로

    Returns:
        WildfireVideoResponse: 업데이트된 비디오 정보
    """
    logger.info(
        f"Updating video - frfr_info_id: {frfr_info_id}, video_name: {video_name}"
    )

    response = service.update_wildfire_video(
        frfr_info_id=frfr_info_id,
        video_name=video_name,
        video_type=video_type,
        video_path=video_path,
    )

    if not response:
        raise HTTPException(
            status_code=404,
            detail=f"Video not found: {frfr_info_id}/{video_name}"
        )

    return response


@router.delete("/videos/{frfr_info_id}/{video_name}")
async def delete_video(
    frfr_info_id: str = Path(..., description="산불 정보 ID"),
    video_name: str = Path(..., description="비디오 이름"),
) -> dict:
    """
    비디오 정보를 삭제합니다.

    Parameters:
        frfr_info_id (str): 산불 정보 ID
        video_name (str): 비디오 이름

    Returns:
        dict: 삭제 결과
    """
    logger.info(
        f"Deleting video - frfr_info_id: {frfr_info_id}, video_name: {video_name}"
    )

    success = service.delete_wildfire_video(frfr_info_id, video_name)

    if not success:
        raise HTTPException(
            status_code=404,
            detail=f"Video not found: {frfr_info_id}/{video_name}"
        )

    return {"message": f"Video deleted: {frfr_info_id}/{video_name}"}


@router.delete("/videos/{frfr_info_id}")
async def delete_all_videos(
    frfr_info_id: str = Path(..., description="산불 정보 ID"),
) -> dict:
    """
    특정 frfr_info_id의 모든 비디오를 삭제합니다.

    Parameters:
        frfr_info_id (str): 산불 정보 ID

    Returns:
        dict: 삭제 결과
    """
    logger.info(f"Deleting all videos for frfr_info_id: {frfr_info_id}")

    success = service.delete_wildfire_videos_by_id(frfr_info_id)

    if not success:
        raise HTTPException(
            status_code=404,
            detail=f"No videos found for: {frfr_info_id}"
        )

    return {"message": f"All videos deleted for: {frfr_info_id}"}


# ==================== JSON 설정 파일 Import 엔드포인트 ====================

@router.post("/import/config")
async def import_from_config_file(
    config_file_path: str = Query(..., description="JSON 설정 파일 경로")
) -> dict:
    """
    JSON 설정 파일을 읽어서 비디오 정보를 DB에 저장합니다.
    
    Example: POST /api/videos/import/config?config_file_path=config/video_config.1.json
    
    Parameters:
        config_file_path (str): JSON 설정 파일의 경로
        
    Returns:
        dict: import 결과
            {
                "success": bool,
                "total": int,
                "imported": int,
                "failed": int,
                "errors": List[str]
            }
    """
    logger.info(f"Importing from config file: {config_file_path}")
    
    result = video_service.import_from_config_file(config_file_path)
    
    if not result["success"]:
        raise HTTPException(
            status_code=400,
            detail=f"Import failed: {', '.join(result['errors'])}"
        )
    
    return result


@router.get("/import/status")
async def get_import_status(
    frfr_info_id: str = Query(..., description="산불 정보 ID")
) -> dict:
    """
    특정 frfr_info_id로 import된 비디오 정보를 조회합니다.
    
    Example: GET /api/videos/import/status?frfr_info_id=123456
    
    Parameters:
        frfr_info_id (str): 산불 정보 ID
        
    Returns:
        dict: 비디오 정보 목록
    """
    logger.info(f"Checking import status for frfr_info_id: {frfr_info_id}")
    
    videos = video_service.get_videos_by_frfr_id(frfr_info_id)
    
    return {
        "frfr_info_id": frfr_info_id,
        "total_videos": len(videos),
        "videos": videos
    }


@router.get("/import/list")
async def list_all_imported_videos() -> dict:
    """
    import된 모든 비디오 정보를 조회합니다.
    
    Example: GET /api/videos/import/list
    
    Returns:
        dict: 모든 비디오 정보
    """
    logger.info("Listing all imported videos")
    
    videos = video_service.get_all_videos()
    
    # frfr_info_id별로 그룹화
    grouped = {}
    for video in videos:
        if video.frfr_info_id not in grouped:
            grouped[video.frfr_info_id] = []
        grouped[video.frfr_info_id].append(video)
    
    return {
        "total_frfr_ids": len(grouped),
        "total_videos": len(videos),
        "data": grouped
    }


@router.get("/import/by-type")
async def get_videos_by_type(
    frfr_info_id: str = Query(..., description="산불 정보 ID"),
    video_type: str = Query(..., description="비디오 타입 (예: FPA630, FPA601)")
) -> dict:
    """
    특정 frfr_info_id와 video_type의 비디오 정보를 조회합니다.
    
    Example: GET /api/videos/import/by-type?frfr_info_id=123456&video_type=FPA630
    
    Parameters:
        frfr_info_id (str): 산불 정보 ID
        video_type (str): 비디오 타입
        
    Returns:
        dict: 필터링된 비디오 정보 목록
    """
    logger.info(
        f"Getting videos by type - frfr_info_id: {frfr_info_id}, "
        f"video_type: {video_type}"
    )
    
    videos = video_service.get_videos_by_type(frfr_info_id, video_type)
    
    return {
        "frfr_info_id": frfr_info_id,
        "video_type": video_type,
        "total_videos": len(videos),
        "videos": videos
    }


# ==================== 비디오 경로 조회 엔드포인트 ====================

@router.get("/paths/by-frfr-id")
async def get_video_paths(
    frfr_info_id: str = Query(..., description="산불 정보 ID")
) -> dict:
    """
    특정 frfr_info_id의 비디오 경로 정보를 조회합니다.
    (video_name과 video_path만 반환)
    
    Example: GET /api/videos/paths/by-frfr-id?frfr_info_id=123456
    
    Parameters:
        frfr_info_id (str): 산불 정보 ID
        
    Returns:
        dict: 비디오 경로 목록
            {
                "frfr_info_id": str,
                "total_videos": int,
                "paths": [
                    {"video_name": str, "video_path": str},
                    ...
                ]
            }
    """
    logger.info(f"Getting video paths for frfr_info_id: {frfr_info_id}")
    
    paths = video_service.get_video_paths_by_frfr_id(frfr_info_id)
    
    return {
        "frfr_info_id": frfr_info_id,
        "total_videos": len(paths),
        "paths": paths
    }


@router.get("/paths/by-type")
async def get_video_paths_by_type(
    frfr_info_id: str = Query(..., description="산불 정보 ID"),
    video_type: str = Query(..., description="비디오 타입 (예: FPA630, FPA601)")
) -> dict:
    """
    특정 frfr_info_id와 video_type의 비디오 경로 정보를 조회합니다.
    (video_name과 video_path만 반환)
    
    Example: GET /api/videos/paths/by-type?frfr_info_id=123456&video_type=FPA630
    
    Parameters:
        frfr_info_id (str): 산불 정보 ID
        video_type (str): 비디오 타입
        
    Returns:
        dict: 필터링된 비디오 경로 목록
            {
                "frfr_info_id": str,
                "video_type": str,
                "total_videos": int,
                "paths": [
                    {"video_name": str, "video_path": str},
                    ...
                ]
            }
    """
    logger.info(
        f"Getting video paths by type - frfr_info_id: {frfr_info_id}, "
        f"video_type: {video_type}"
    )
    
    paths = video_service.get_video_paths_by_type(frfr_info_id, video_type)
    
    return {
        "frfr_info_id": frfr_info_id,
        "video_type": video_type,
        "total_videos": len(paths),
        "paths": paths
    }


@router.get("/paths/all")
async def get_all_video_paths() -> dict:
    """
    모든 비디오의 경로 정보를 조회합니다.
    (video_name과 video_path만 반환)
    
    Example: GET /api/videos/paths/all
    
    Returns:
        dict: 모든 비디오의 경로 목록
            {
                "total_videos": int,
                "paths": [
                    {"video_name": str, "video_path": str},
                    ...
                ]
            }
    """
    logger.info("Getting all video paths")
    
    paths = video_service.get_all_video_paths()
    
    return {
        "total_videos": len(paths),
        "paths": paths
    }