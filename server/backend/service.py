from typing import Optional, List, Dict, Any
from server.backend.db.database import (
    get_shared_database,
    WildfireTable,
    WildfireVideoTable,
)
from server.backend.db.table.analysis_status_table import AnalysisTable
from server.response_obj import WildfireResponse, FireLocation, VideoInfo
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)


# ==================== Pydantic Models ====================

class WildfireVideoCreate(BaseModel):
    """비디오 생성 요청 모델"""
    frfr_info_id: str
    video_name: str
    video_type: str
    video_path: str


class WildfireVideoResponse(BaseModel):
    """비디오 응답 모델"""
    frfr_info_id: str
    video_name: str
    video_type: str
    video_path: str


class AnalysisCreate(BaseModel):
    """분석 생성 요청 모델"""
    analysis_id: str
    frfr_info_id: str
    video_updates: List[Dict[str, str]]


class AnalysisResponse(BaseModel):
    """분석 응답 모델"""
    analysis_id: str
    frfr_info_id: str
    video_updates: List[Dict[str, str]]


class WildfireService:
    """산불 데이터 처리 서비스"""

    def __init__(self):
        shared_db = get_shared_database()
        self.wildfire_table = WildfireTable(shared_db)
        self.wildfire_video_table = WildfireVideoTable(shared_db)
        self.analysis_table = AnalysisTable(shared_db)

    # ==================== Wildfire 테이블 메서드 ====================

    def create_wildfire(
        self,
        frfr_info_id: str,
        analysis_id: str,
        fire_location: Dict[str, float],
        videos: List[Dict[str, Any]],
    ) -> WildfireResponse:
        """
        새로운 산불 정보를 생성하고 저장합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            fire_location: 위치 정보
            videos: 비디오 목록

        Returns:
            WildfireResponse 객체
        """
        existing = self.wildfire_table.get(frfr_info_id)
        if existing:
            logger.warning(f"Wildfire record already exists: {frfr_info_id}")

        self.wildfire_table.insert(
            frfr_info_id=frfr_info_id,
            analysis_id=analysis_id,
            fire_location=fire_location,
            videos=videos,
        )

        return self._build_response(
            frfr_info_id, analysis_id, fire_location, videos
        )

    def get_wildfire(self, frfr_info_id: str) -> Optional[WildfireResponse]:
        """
        산불 정보를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            WildfireResponse 객체 또는 None
        """
        data = self.wildfire_table.get(frfr_info_id)
        if not data:
            return None

        return self._build_response(
            data["frfr_info_id"],
            data["analysis_id"],
            data["fire_location"],
            data["videos"],
        )

    def update_wildfire(
        self,
        frfr_info_id: str,
        fire_location: Optional[Dict[str, float]] = None,
        videos: Optional[List[Dict[str, Any]]] = None,
        analysis_id: Optional[str] = None,
    ) -> Optional[WildfireResponse]:
        """
        산불 정보를 업데이트합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            fire_location: 업데이트할 위치 정보
            videos: 업데이트할 비디오 목록
            analysis_id: 업데이트할 분석 ID

        Returns:
            업데이트된 WildfireResponse 객체 또는 None
        """
        success = self.wildfire_table.update(
            frfr_info_id=frfr_info_id,
            fire_location=fire_location,
            videos=videos,
            analysis_id=analysis_id,
        )

        if not success:
            return None

        return self.get_wildfire(frfr_info_id)

    def delete_wildfire(self, frfr_info_id: str) -> bool:
        """
        산불 정보를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        return self.wildfire_table.delete(frfr_info_id)

    def get_all_wildfires(self) -> List[WildfireResponse]:
        """
        모든 산불 정보를 조회합니다.

        Returns:
            WildfireResponse 객체 리스트
        """
        all_data = self.wildfire_table.get_all()
        return [
            self._build_response(
                data["frfr_info_id"],
                data["analysis_id"],
                data["fire_location"],
                data["videos"],
            )
            for data in all_data
        ]

    def add_video_to_wildfire(
        self, frfr_info_id: str, video_info: Dict[str, Any]
    ) -> Optional[WildfireResponse]:
        """
        산불 레코드에 비디오를 추가합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_info: 추가할 비디오 정보

        Returns:
            업데이트된 WildfireResponse 객체 또는 None
        """
        success = self.wildfire_table.add_video(frfr_info_id, video_info)
        if not success:
            return None

        return self.get_wildfire(frfr_info_id)

    def remove_video_from_wildfire(
        self, frfr_info_id: str, video_url: str
    ) -> Optional[WildfireResponse]:
        """
        산불 레코드에서 비디오를 제거합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_url: 제거할 비디오 URL

        Returns:
            업데이트된 WildfireResponse 객체 또는 None
        """
        success = self.wildfire_table.remove_video(frfr_info_id, video_url)
        if not success:
            return None

        return self.get_wildfire(frfr_info_id)

    # ==================== Wildfire Video 테이블 메서드 ====================

    def create_wildfire_video(
        self,
        frfr_info_id: str,
        video_name: str,
        video_type: str,
        video_path: str,
    ) -> WildfireVideoResponse:
        """
        새로운 산불 비디오 정보를 저장합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_name: 비디오 이름
            video_type: 비디오 타입 (예: FPA601, FPA630)
            video_path: 비디오 경로

        Returns:
            WildfireVideoResponse 객체
        """
        self.wildfire_video_table.insert(
            frfr_info_id=frfr_info_id,
            video_name=video_name,
            video_type=video_type,
            video_path=video_path,
        )

        return WildfireVideoResponse(
            frfr_info_id=frfr_info_id,
            video_name=video_name,
            video_type=video_type,
            video_path=video_path,
        )

    def get_wildfire_video(
        self, frfr_info_id: str, video_name: str
    ) -> Optional[WildfireVideoResponse]:
        """
        frfr_info_id와 video_name으로 비디오 정보를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_name: 비디오 이름

        Returns:
            WildfireVideoResponse 객체 또는 None
        """
        data = self.wildfire_video_table.get(frfr_info_id, video_name)
        if not data:
            return None

        return WildfireVideoResponse(
            frfr_info_id=data["frfr_info_id"],
            video_name=data["video_name"],
            video_type=data["video_type"],
            video_path=data["video_path"],
        )

    def get_wildfire_videos(
        self, frfr_info_id: str
    ) -> List[WildfireVideoResponse]:
        """
        특정 frfr_info_id의 모든 비디오를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            WildfireVideoResponse 객체 리스트
        """
        videos = self.wildfire_video_table.get_by_frfr_id(frfr_info_id)
        return [
            WildfireVideoResponse(
                frfr_info_id=v["frfr_info_id"],
                video_name=v["video_name"],
                video_type=v["video_type"],
                video_path=v["video_path"],
            )
            for v in videos
        ]

    def update_wildfire_video(
        self,
        frfr_info_id: str,
        video_name: str,
        video_type: Optional[str] = None,
        video_path: Optional[str] = None,
    ) -> Optional[WildfireVideoResponse]:
        """
        비디오 정보를 업데이트합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_name: 비디오 이름
            video_type: 업데이트할 비디오 타입
            video_path: 업데이트할 비디오 경로

        Returns:
            업데이트된 WildfireVideoResponse 객체 또는 None
        """
        success = self.wildfire_video_table.update(
            frfr_info_id=frfr_info_id,
            video_name=video_name,
            video_type=video_type,
            video_path=video_path,
        )

        if not success:
            return None

        return self.get_wildfire_video(frfr_info_id, video_name)

    def delete_wildfire_video(
        self, frfr_info_id: str, video_name: str
    ) -> bool:
        """
        비디오 정보를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_name: 비디오 이름

        Returns:
            삭제 성공 여부
        """
        return self.wildfire_video_table.delete(frfr_info_id, video_name)

    def delete_wildfire_videos_by_id(self, frfr_info_id: str) -> bool:
        """
        특정 frfr_info_id의 모든 비디오를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        return self.wildfire_video_table.delete_by_frfr_id(frfr_info_id)

    @staticmethod
    def _build_response(
        frfr_info_id: str,
        analysis_id: str,
        fire_location: Dict[str, float],
        videos: List[Dict[str, Any]],
    ) -> WildfireResponse:
        """
        데이터베이스 데이터를 WildfireResponse 객체로 변환합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            fire_location: 위치 정보
            videos: 비디오 목록

        Returns:
            WildfireResponse 객체
        """
        location = FireLocation(
            latitude=fire_location["latitude"],
            longitude=fire_location["longitude"],
        )

        video_list = [
            VideoInfo(
                path=v.get("path", ""),
                name=v.get("name", ""),
                add_time=v.get("add_time", ""),
                analysis_status=v.get("analysis_status", ""),
            )
            for v in videos
        ]

        return WildfireResponse(
            frfr_info_id=frfr_info_id,
            analysis_id=analysis_id,
            fire_location=location,
            videos=video_list,
        )

    # ==================== Analysis 테이블 메서드 ====================

    def create_analysis(
        self,
        analysis_id: str,
        frfr_info_id: str,
        video_updates: List[Dict[str, str]],
    ) -> AnalysisResponse:
        """
        새로운 분석 정보를 생성하고 저장합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            video_updates: 비디오 업데이트 리스트

        Returns:
            AnalysisResponse 객체
        """
        self.analysis_table.insert(
            analysis_id=analysis_id,
            frfr_info_id=frfr_info_id,
            video_updates=video_updates,
        )

        return AnalysisResponse(
            analysis_id=analysis_id,
            frfr_info_id=frfr_info_id,
            video_updates=video_updates,
        )

    def get_analysis(
        self, analysis_id: str, frfr_info_id: str
    ) -> Optional[AnalysisResponse]:
        """
        분석 정보를 조회합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            AnalysisResponse 객체 또는 None
        """
        data = self.analysis_table.get(analysis_id, frfr_info_id)
        if not data:
            return None

        return AnalysisResponse(
            analysis_id=data["analysis_id"],
            frfr_info_id=data["frfr_info_id"],
            video_updates=data["video_updates"],
        )

    def get_analyses_by_analysis_id(
        self, analysis_id: str
    ) -> List[AnalysisResponse]:
        """
        특정 analysis_id의 모든 분석을 조회합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            AnalysisResponse 객체 리스트
        """
        analyses = self.analysis_table.get_by_analysis_id(analysis_id)
        return [
            AnalysisResponse(
                analysis_id=a["analysis_id"],
                frfr_info_id=a["frfr_info_id"],
                video_updates=a["video_updates"],
            )
            for a in analyses
        ]

    def get_analyses_by_frfr_id(
        self, frfr_info_id: str
    ) -> List[AnalysisResponse]:
        """
        특정 frfr_info_id의 모든 분석을 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            AnalysisResponse 객체 리스트
        """
        analyses = self.analysis_table.get_by_frfr_id(frfr_info_id)
        return [
            AnalysisResponse(
                analysis_id=a["analysis_id"],
                frfr_info_id=a["frfr_info_id"],
                video_updates=a["video_updates"],
            )
            for a in analyses
        ]

    def update_analysis(
        self,
        analysis_id: str,
        frfr_info_id: str,
        video_updates: Optional[List[Dict[str, str]]] = None,
    ) -> Optional[AnalysisResponse]:
        """
        분석 정보를 업데이트합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            video_updates: 업데이트할 비디오 업데이트 리스트

        Returns:
            업데이트된 AnalysisResponse 객체 또는 None
        """
        success = self.analysis_table.update(
            analysis_id=analysis_id,
            frfr_info_id=frfr_info_id,
            video_updates=video_updates,
        )

        if not success:
            return None

        return self.get_analysis(analysis_id, frfr_info_id)

    def delete_analysis(
        self, analysis_id: str, frfr_info_id: str
    ) -> bool:
        """
        분석 정보를 삭제합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        return self.analysis_table.delete(analysis_id, frfr_info_id)

    def delete_analyses_by_analysis_id(self, analysis_id: str) -> bool:
        """
        특정 analysis_id의 모든 분석을 삭제합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            삭제 성공 여부
        """
        return self.analysis_table.delete_by_analysis_id(analysis_id)

    def delete_analyses_by_frfr_id(self, frfr_info_id: str) -> bool:
        """
        특정 frfr_info_id의 모든 분석을 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        return self.analysis_table.delete_by_frfr_id(frfr_info_id)

    def add_video_update_to_analysis(
        self,
        analysis_id: str,
        frfr_info_id: str,
        video_name: str,
        analysis_status: str,
    ) -> Optional[AnalysisResponse]:
        """
        분석 레코드에 비디오 업데이트를 추가합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            video_name: 비디오 이름
            analysis_status: 분석 상태

        Returns:
            업데이트된 AnalysisResponse 객체 또는 None
        """
        success = self.analysis_table.add_video_update(
            analysis_id=analysis_id,
            frfr_info_id=frfr_info_id,
            video_name=video_name,
            analysis_status=analysis_status,
        )

        if not success:
            return None

        return self.get_analysis(analysis_id, frfr_info_id)

    def remove_video_update_from_analysis(
        self, analysis_id: str, frfr_info_id: str, video_name: str
    ) -> Optional[AnalysisResponse]:
        """
        분석 레코드에서 비디오 업데이트를 제거합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            video_name: 제거할 비디오 이름

        Returns:
            업데이트된 AnalysisResponse 객체 또는 None
        """
        success = self.analysis_table.remove_video_update(
            analysis_id=analysis_id,
            frfr_info_id=frfr_info_id,
            video_name=video_name,
        )

        if not success:
            return None

        return self.get_analysis(analysis_id, frfr_info_id)

    def get_all_analyses(self) -> List[AnalysisResponse]:
        """
        모든 분석 정보를 조회합니다.

        Returns:
            AnalysisResponse 객체 리스트
        """
        all_data = self.analysis_table.get_all()
        return [
            AnalysisResponse(
                analysis_id=a["analysis_id"],
                frfr_info_id=a["frfr_info_id"],
                video_updates=a["video_updates"],
            )
            for a in all_data
        ]


# 싱글톤 인스턴스
_service_instance = None


def get_wildfire_service() -> WildfireService:
    """산불 서비스 싱글톤 인스턴스를 반환합니다."""
    global _service_instance
    if _service_instance is None:
        _service_instance = WildfireService()
    return _service_instance