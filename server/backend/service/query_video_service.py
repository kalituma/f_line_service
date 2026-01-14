import logging
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from server.backend.db.database import get_shared_database, SharedDatabase
from server.backend.service.fire_info_service import FireInfoService
from server.backend.service.video_service import WildfireVideoService
from server.backend.service.analysis_status_service import AnalysisStatusService
from server.response_obj import VideoStatusUpdate

logger = logging.getLogger(__name__)


class FireLocation(BaseModel):
    """산불 위치 정보 모델"""
    latitude: float
    longitude: float


class VideoInfo(BaseModel):
    """비디오 정보 모델"""
    video_url: str
    video_name: str
    add_time: Optional[str] = None
    analysis_status: str


class QueryVideoResponse(BaseModel):
    """쿼리 응답 모델"""
    frfr_info_id: str
    analysis_id: str
    fire_location: FireLocation
    videos: List[VideoInfo]


class QueryVideoService:
    """frfr_info_id와 analysis_id를 기반으로 통합 정보를 조회하는 서비스"""

    def __init__(self, db: SharedDatabase = None):
        """
        서비스 초기화

        Args:
            db: SharedDatabase 인스턴스 (선택사항)
        """
        if db is None:
            db = get_shared_database()
        
        self.db = db
        self.fire_info_service = FireInfoService(db)
        self.video_service = WildfireVideoService(db)
        self.analysis_status_service = AnalysisStatusService(db)
        logger.info("QueryVideoService initialized")

    def get_query_data(
        self, frfr_info_id: str, analysis_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        frfr_info_id와 analysis_id를 기반으로 통합 정보를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID

        Returns:
            통합 정보 딕셔너리 또는 None
            {
                "frfr_info_id": "186525",
                "analysis_id": "202511271004_VIDEO_005",
                "fire_location": {
                    "latitude": 36.30740755588261,
                    "longitude": 128.45275734365313
                },
                "videos": [
                    {
                        "path": "/data/helivid/hv_proc/186525/FPA601/...",
                        "name": "FPA630_20251127_190145_7366_000.mkv",
                        "add_time": "2025-11-27 10:03:47.550",
                        "analysis_status": "STAT_001"
                    },
                    ...
                ]
            }
        """
        try:
            logger.info(
                f"Getting query data for frfr_info_id={frfr_info_id}, "
                f"analysis_id={analysis_id}"
            )

            # 1. 산불 위치 정보 조회
            fire_location_data = self.fire_info_service.get_fire_location(frfr_info_id)
            if not fire_location_data:
                logger.warning(f"Fire location not found for {frfr_info_id}")
                return None

            fire_location_info = fire_location_data.get("fire_location", {})
            fire_location = {
                "latitude": fire_location_info.get("latitude"),
                "longitude": fire_location_info.get("longitude"),
            }

            # 2. 분석 상태 정보 조회
            analysis_status_list = (
                self.analysis_status_service.get_all_status_by_analysis_id(analysis_id)
            )
            
            if not analysis_status_list:
                logger.warning(
                    f"No analysis status found for analysis_id={analysis_id}"
                )
                return None

            # 3. 비디오 정보 구성
            videos = []
            for analysis_status in analysis_status_list:
                video_name = analysis_status.get("video_name")
                status = analysis_status.get("analysis_status")

                # 비디오 경로 정보 조회
                video_paths = self.video_service.get_video_paths_by_frfr_id(
                    frfr_info_id
                )
                
                # video_name과 일치하는 비디오 찾기
                video_path = None
                for vp in video_paths:
                    if vp.get("video_name") == video_name:
                        video_path = vp.get("video_path")
                        break

                # 비디오 상세 정보 조회
                video_add_time = None
                if video_path:
                    videos_list = self.video_service.get_videos_by_frfr_id(frfr_info_id)
                    for video in videos_list:
                        if video.video_name == video_name:
                            video_add_time = video.add_time
                            break

                video_info = {
                    "video_url": video_path or "",
                    "video_name": video_name,
                    "add_time": video_add_time,
                    "analysis_status": status,
                }
                videos.append(video_info)

            # 4. 응답 데이터 구성
            response = {
                "frfr_info_id": frfr_info_id,
                "analysis_id": analysis_id,
                "fire_location": fire_location,
                "videos": videos,
            }

            logger.info(
                f"Successfully retrieved query data: "
                f"{len(videos)} videos found"
            )
            return response

        except Exception as e:
            logger.error(
                f"Failed to get query data for {frfr_info_id}/{analysis_id}: "
                f"{str(e)}"
            )
            return None

    def get_query_data_as_object(
        self, frfr_info_id: str, analysis_id: str
    ) -> Optional[QueryVideoResponse]:
        """
        frfr_info_id와 analysis_id를 기반으로 통합 정보를 조회합니다.
        Pydantic 모델로 반환합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID

        Returns:
            QueryVideoResponse 객체 또는 None
        """
        try:
            data = self.get_query_data(frfr_info_id, analysis_id)
            if data is None:
                return None

            # Pydantic 모델로 변환
            response = QueryVideoResponse(
                frfr_info_id=data["frfr_info_id"],
                analysis_id=data["analysis_id"],
                fire_location=FireLocation(**data["fire_location"]),
                videos=[VideoInfo(**video) for video in data["videos"]],
            )
            return response

        except Exception as e:
            logger.error(f"Failed to convert to QueryVideoResponse: {str(e)}")
            return None

    def check_video_exists(
        self, frfr_info_id: str, video_updates: List[VideoStatusUpdate]
    ) -> Dict[str, Any]:
        """
        frfr_info_id와 video_updates 리스트의 각 비디오가 DB에 존재하는지 확인합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_updates: 비디오 상태 업데이트 리스트

        Returns:
            {
                "all_exist": bool (모든 비디오 존재 여부),
                "frfr_info_exists": bool (산불 정보 존재 여부),
                "videos": [
                    {
                        "video_name": str,
                        "exists": bool
                    },
                    ...
                ]
            }
        """
        try:
            logger.info(
                f"Checking data existence for frfr_info_id={frfr_info_id}, "
                f"video_count={len(video_updates)}"
            )

            result = {
                "all_exist": False,
                "frfr_info_exists": False,
                "videos": []
            }

            # 1. 산불 정보 존재 확인
            fire_location_exists = self.fire_info_service.check_fire_location_exists(frfr_info_id)
            result["frfr_info_exists"] = fire_location_exists

            if not fire_location_exists:
                logger.warning(f"Fire location not found for {frfr_info_id}")
                return result

            # 2. 각 비디오가 존재하는지 확인
            all_videos_exist = True
            for video_update in video_updates:
                video_name = video_update.video_name
                video_exists = self.video_service.check_video_exists_by_name(
                    frfr_info_id=frfr_info_id,
                    video_name=video_name
                )
                
                result["videos"].append({
                    "video_name": video_name,
                    "exists": video_exists
                })

                if not video_exists:
                    all_videos_exist = False
                    logger.warning(
                        f"Video not found: frfr_info_id={frfr_info_id}, "
                        f"video_name={video_name}"
                    )

            result["all_exist"] = all_videos_exist

            logger.info(
                f"Data check completed: frfr_info_exists={result['frfr_info_exists']}, "
                f"all_videos_exist={all_videos_exist}, "
                f"total_videos={len(video_updates)}"
            )

            return result

        except Exception as e:
            logger.error(
                f"Failed to check data existence for frfr_info_id={frfr_info_id}: "
                f"{str(e)}"
            )
            return {
                "all_exist": False,
                "frfr_info_exists": False,
                "videos": []
            }
