import logging
from datetime import datetime
from tinydb import Query
from typing import Optional, List, Dict, Any

from server.backend.db.database import get_shared_database, SharedDatabase
from server.utils.result_handling import result_without_id

logger = logging.getLogger(__name__)

class WildfireVideoTable:
    """Wildfire Video 테이블 관리 (복합키: frfr_info_id + analysis_id + video_name)
    table structure = {
        "frfr_info_id": str,
        "analysis_id": str,
        "video_name": str,
        "video_type": str,
        "video_path": str,
        "add_time": str,  # ISO format datetime string
    }
    """

    def __init__(self, db: SharedDatabase = None):
        if db is None:
            db = get_shared_database()
        self.db = db
        self.table = db.get_table("wildfire_video")
        logger.info("WildfireVideoTable initialized")

    def insert(
        self,
        frfr_info_id: str,
        analysis_id: str,
        video_name: str,
        video_type: str,
        video_path: str,
        add_time: Optional[str] = None,
    ) -> str:
        """
        산불 비디오 정보를 저장합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID (복합키의 일부)
            video_name: 비디오 이름 (복합키의 일부)
            video_type: 비디오 타입 (예: FPA601, FPA630)
            video_path: 비디오 경로
            add_time: 비디오 생성 시간 (ISO format, 미지정시 현재 시간)

        Returns:
            저장된 레코드 ID
        """
        data = {
            "frfr_info_id": frfr_info_id,
            "analysis_id": analysis_id,
            "video_name": video_name,
            "video_type": video_type,
            "video_path": video_path,
            "add_time": add_time or datetime.now().isoformat(),
        }
        doc_id = self.table.insert(data)
        logger.info(
            f"Inserted wildfire_video record: {frfr_info_id}/{analysis_id}/{video_name}"
        )
        return str(doc_id)

    def get(
        self, frfr_info_id: str, analysis_id: str, video_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        frfr_info_id, analysis_id, video_name 복합키로 비디오 정보를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            video_name: 비디오 이름

        Returns:
            비디오 정보 딕셔너리 또는 None
        """
        video = Query()
        result = self.table.get(
            (video.frfr_info_id == frfr_info_id)
            & (video.analysis_id == analysis_id)
            & (video.video_name == video_name)
        )
        if result:
            logger.info(
                f"Found wildfire_video record: {frfr_info_id}/{analysis_id}/{video_name}"
            )
        else:
            logger.warning(
                f"Wildfire_video record not found: {frfr_info_id}/{analysis_id}/{video_name}"
            )
        return result

    def get_by_frfr_id(
        self, frfr_info_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 frfr_info_id의 모든 비디오를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            비디오 정보 리스트
        """
        video = Query()
        results = self.table.search(video.frfr_info_id == frfr_info_id)
        logger.info(
            f"Retrieved {len(results)} wildfire_video records for {frfr_info_id}"
        )
        return result_without_id(results)

    def get_by_frfr_id_and_analysis_id(
        self, frfr_info_id: str, analysis_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 frfr_info_id와 analysis_id의 모든 비디오를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID

        Returns:
            비디오 정보 리스트
        """
        video = Query()
        results = self.table.search(
            (video.frfr_info_id == frfr_info_id)
            & (video.analysis_id == analysis_id)
        )
        logger.info(
            f"Retrieved {len(results)} wildfire_video records for {frfr_info_id}/{analysis_id}"
        )
        return results

    def update(
        self,
        frfr_info_id: str,
        analysis_id: str,
        video_name: str,
        video_type: Optional[str] = None,
        video_path: Optional[str] = None,
        add_time: Optional[str] = None,
    ) -> bool:
        """
        비디오 정보를 업데이트합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            video_name: 비디오 이름
            video_type: 업데이트할 비디오 타입
            video_path: 업데이트할 비디오 경로
            add_time: 업데이트할 비디오 생성 시간 (ISO format)

        Returns:
            업데이트 성공 여부
        """
        video = Query()
        update_data = {}

        if video_type:
            update_data["video_type"] = video_type
        if video_path:
            update_data["video_path"] = video_path
        if add_time:
            update_data["add_time"] = add_time

        if not update_data:
            logger.warning("No data to update")
            return False

        result = self.table.update(
            update_data,
            (video.frfr_info_id == frfr_info_id)
            & (video.analysis_id == analysis_id)
            & (video.video_name == video_name),
        )

        if result:
            logger.info(
                f"Updated wildfire_video record: {frfr_info_id}/{analysis_id}/{video_name}"
            )
            return True
        else:
            logger.warning(
                f"Failed to update wildfire_video record: "
                f"{frfr_info_id}/{analysis_id}/{video_name}"
            )
            return False

    def delete(
        self, frfr_info_id: str, analysis_id: str, video_name: str
    ) -> bool:
        """
        비디오 정보를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            video_name: 비디오 이름

        Returns:
            삭제 성공 여부
        """
        video = Query()
        result = self.table.remove(
            (video.frfr_info_id == frfr_info_id)
            & (video.analysis_id == analysis_id)
            & (video.video_name == video_name)
        )

        if result:
            logger.info(
                f"Deleted wildfire_video record: {frfr_info_id}/{analysis_id}/{video_name}"
            )
            return True
        else:
            logger.warning(
                f"Failed to delete wildfire_video record: "
                f"{frfr_info_id}/{analysis_id}/{video_name}"
            )
            return False

    def delete_by_frfr_id(self, frfr_info_id: str) -> bool:
        """
        특정 frfr_info_id의 모든 비디오를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        video = Query()
        result = self.table.remove(video.frfr_info_id == frfr_info_id)

        if result:
            logger.info(
                f"Deleted all wildfire_video records for {frfr_info_id}"
            )
            return True
        else:
            logger.warning(
                f"No wildfire_video records to delete for {frfr_info_id}"
            )
            return False

    def delete_by_frfr_id_and_analysis_id(
        self, frfr_info_id: str, analysis_id: str
    ) -> bool:
        """
        특정 frfr_info_id와 analysis_id의 모든 비디오를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID

        Returns:
            삭제 성공 여부
        """
        video = Query()
        result = self.table.remove(
            (video.frfr_info_id == frfr_info_id)
            & (video.analysis_id == analysis_id)
        )

        if result:
            logger.info(
                f"Deleted all wildfire_video records for {frfr_info_id}/{analysis_id}"
            )
            return True
        else:
            logger.warning(
                f"No wildfire_video records to delete for {frfr_info_id}/{analysis_id}"
            )
            return False

    def get_all(self) -> List[Dict[str, Any]]:
        """
        모든 비디오 정보를 조회합니다.

        Returns:
            비디오 정보 리스트
        """
        results = self.table.all()
        logger.info(f"Retrieved {len(results)} total wildfire_video records")
        return results