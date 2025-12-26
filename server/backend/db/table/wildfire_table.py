import logging
from tinydb import Query
from typing import Optional, List, Dict, Any

from server.backend.db.database import get_shared_database, SharedDatabase

logger = logging.getLogger(__name__)

class WildfireTable:
    """Wildfire 테이블 관리"""

    def __init__(self, db: SharedDatabase = None):
        if db is None:
            db = get_shared_database()
        self.db = db
        self.table = db.get_table("wildfire")
        logger.info("WildfireTable initialized")

    def insert(
        self,
        frfr_info_id: str,
        analysis_id: str,
        fire_location: Dict[str, float],
        videos: List[Dict[str, Any]],
    ) -> str:
        """
        산불 정보를 데이터베이스에 저장합니다.

        Args:
            frfr_info_id: 산불 정보 ID (Primary Key)
            analysis_id: 분석 ID
            fire_location: 위치 정보 {'latitude': float, 'longitude': float}
            videos: 비디오 목록

        Returns:
            저장된 레코드 ID
        """
        data = {
            "frfr_info_id": frfr_info_id,
            "analysis_id": analysis_id,
            "fire_location": fire_location,
            "videos": videos,
        }
        doc_id = self.table.insert(data)
        logger.info(f"Inserted wildfire record: {frfr_info_id}")
        return str(doc_id)

    def get(self, frfr_info_id: str) -> Optional[Dict[str, Any]]:
        """
        주어진 frfr_info_id로 산불 정보를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            산불 정보 딕셔너리 또는 None
        """
        wildfire = Query()
        result = self.table.get(wildfire.frfr_info_id == frfr_info_id)
        if result:
            logger.info(f"Found wildfire record: {frfr_info_id}")
        else:
            logger.warning(f"Wildfire record not found: {frfr_info_id}")
        return result

    def update(
        self,
        frfr_info_id: str,
        fire_location: Optional[Dict[str, float]] = None,
        videos: Optional[List[Dict[str, Any]]] = None,
        analysis_id: Optional[str] = None,
    ) -> bool:
        """
        산불 정보를 업데이트합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            fire_location: 업데이트할 위치 정보
            videos: 업데이트할 비디오 목록
            analysis_id: 업데이트할 분석 ID

        Returns:
            업데이트 성공 여부
        """
        wildfire = Query()
        update_data = {}

        if fire_location:
            update_data["fire_location"] = fire_location
        if videos:
            update_data["videos"] = videos
        if analysis_id:
            update_data["analysis_id"] = analysis_id

        if not update_data:
            logger.warning("No data to update")
            return False

        result = self.table.update(
            update_data, wildfire.frfr_info_id == frfr_info_id
        )

        if result:
            logger.info(f"Updated wildfire record: {frfr_info_id}")
            return True
        else:
            logger.warning(f"Failed to update wildfire record: {frfr_info_id}")
            return False

    def delete(self, frfr_info_id: str) -> bool:
        """
        산불 정보를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        wildfire = Query()
        result = self.table.remove(wildfire.frfr_info_id == frfr_info_id)

        if result:
            logger.info(f"Deleted wildfire record: {frfr_info_id}")
            return True
        else:
            logger.warning(f"Failed to delete wildfire record: {frfr_info_id}")
            return False

    def get_all(self) -> List[Dict[str, Any]]:
        """
        모든 산불 정보를 조회합니다.

        Returns:
            산불 정보 리스트
        """
        results = self.table.all()
        logger.info(f"Retrieved {len(results)} wildfire records")
        return results

    def add_video(
        self, frfr_info_id: str, video_info: Dict[str, Any]
    ) -> bool:
        """
        기존 산불 레코드에 비디오를 추가합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_info: 추가할 비디오 정보

        Returns:
            추가 성공 여부
        """
        wildfire = self.get(frfr_info_id)
        if not wildfire:
            logger.warning(f"Wildfire record not found: {frfr_info_id}")
            return False

        wildfire["videos"].append(video_info)
        return self.update(frfr_info_id, videos=wildfire["videos"])

    def remove_video(
        self, frfr_info_id: str, video_url: str
    ) -> bool:
        """
        산불 레코드에서 비디오를 제거합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_url: 제거할 비디오 URL

        Returns:
            제거 성공 여부
        """
        wildfire = self.get(frfr_info_id)
        if not wildfire:
            logger.warning(f"Wildfire record not found: {frfr_info_id}")
            return False

        original_count = len(wildfire["videos"])
        wildfire["videos"] = [
            v for v in wildfire["videos"] if v.get("video_url") != video_url
        ]

        if len(wildfire["videos"]) < original_count:
            return self.update(frfr_info_id, videos=wildfire["videos"])
        else:
            logger.warning(f"Video not found: {video_url}")
            return False