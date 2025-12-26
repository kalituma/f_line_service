import logging
from tinydb import Query
from typing import Optional, List, Dict, Any

from server.backend.db.database import get_shared_database, SharedDatabase

logger = logging.getLogger(__name__)


class AnalysisTable:
    """Analysis 테이블 관리 (복합키: analysis_id + frfr_info_id)"""

    def __init__(self, db: SharedDatabase = None):
        if db is None:
            db = get_shared_database()
        self.db = db
        self.table = db.get_table("analysis_status")
        logger.info("AnalysisTable initialized")

    def insert(
        self,
        analysis_id: str,
        frfr_info_id: str,
        video_updates: List[Dict[str, str]],
    ) -> str:
        """
        분석 정보를 저장합니다.

        Args:
            analysis_id: 분석 ID (Primary Key)
            frfr_info_id: 산불 정보 ID (Foreign Key)
            video_updates: 비디오 업데이트 리스트
                [{"video_name": str, "analysis_status": str}, ...]

        Returns:
            저장된 레코드 ID
        """
        data = {
            "analysis_id": analysis_id,
            "frfr_info_id": frfr_info_id,
            "video_updates": video_updates,
        }
        doc_id = self.table.insert(data)
        logger.info(
            f"Inserted analysis record: {analysis_id}/{frfr_info_id}"
        )
        return str(doc_id)

    def get(
        self, analysis_id: str, frfr_info_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        analysis_id와 frfr_info_id 복합키로 분석 정보를 조회합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            분석 정보 딕셔너리 또는 None
        """
        analysis = Query()
        result = self.table.get(
            (analysis.analysis_id == analysis_id)
            & (analysis.frfr_info_id == frfr_info_id)
        )
        if result:
            logger.info(
                f"Found analysis record: {analysis_id}/{frfr_info_id}"
            )
        else:
            logger.warning(
                f"Analysis record not found: {analysis_id}/{frfr_info_id}"
            )
        return result

    def get_by_analysis_id(
        self, analysis_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 analysis_id의 모든 분석을 조회합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            분석 정보 리스트
        """
        analysis = Query()
        results = self.table.search(analysis.analysis_id == analysis_id)
        logger.info(
            f"Retrieved {len(results)} analysis records for {analysis_id}"
        )
        return results

    def get_by_frfr_id(
        self, frfr_info_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 frfr_info_id의 모든 분석을 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            분석 정보 리스트
        """
        analysis = Query()
        results = self.table.search(analysis.frfr_info_id == frfr_info_id)
        logger.info(
            f"Retrieved {len(results)} analysis records for {frfr_info_id}"
        )
        return results

    def update(
        self,
        analysis_id: str,
        frfr_info_id: str,
        video_updates: Optional[List[Dict[str, str]]] = None,
    ) -> bool:
        """
        분석 정보를 업데이트합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            video_updates: 업데이트할 비디오 업데이트 리스트

        Returns:
            업데이트 성공 여부
        """
        analysis = Query()
        update_data = {}

        if video_updates:
            update_data["video_updates"] = video_updates

        if not update_data:
            logger.warning("No data to update")
            return False

        result = self.table.update(
            update_data,
            (analysis.analysis_id == analysis_id)
            & (analysis.frfr_info_id == frfr_info_id),
        )

        if result:
            logger.info(
                f"Updated analysis record: {analysis_id}/{frfr_info_id}"
            )
            return True
        else:
            logger.warning(
                f"Failed to update analysis record: "
                f"{analysis_id}/{frfr_info_id}"
            )
            return False

    def delete(
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
        analysis = Query()
        result = self.table.remove(
            (analysis.analysis_id == analysis_id)
            & (analysis.frfr_info_id == frfr_info_id)
        )

        if result:
            logger.info(
                f"Deleted analysis record: {analysis_id}/{frfr_info_id}"
            )
            return True
        else:
            logger.warning(
                f"Failed to delete analysis record: "
                f"{analysis_id}/{frfr_info_id}"
            )
            return False

    def delete_by_analysis_id(self, analysis_id: str) -> bool:
        """
        특정 analysis_id의 모든 분석을 삭제합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            삭제 성공 여부
        """
        analysis = Query()
        result = self.table.remove(analysis.analysis_id == analysis_id)

        if result:
            logger.info(
                f"Deleted all analysis records for {analysis_id}"
            )
            return True
        else:
            logger.warning(
                f"No analysis records to delete for {analysis_id}"
            )
            return False

    def delete_by_frfr_id(self, frfr_info_id: str) -> bool:
        """
        특정 frfr_info_id의 모든 분석을 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        analysis = Query()
        result = self.table.remove(analysis.frfr_info_id == frfr_info_id)

        if result:
            logger.info(
                f"Deleted all analysis records for {frfr_info_id}"
            )
            return True
        else:
            logger.warning(
                f"No analysis records to delete for {frfr_info_id}"
            )
            return False

    def add_video_update(
        self,
        analysis_id: str,
        frfr_info_id: str,
        video_name: str,
        analysis_status: str,
    ) -> bool:
        """
        분석 레코드에 비디오 업데이트를 추가합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            video_name: 비디오 이름
            analysis_status: 분석 상태

        Returns:
            추가 성공 여부
        """
        analysis_record = self.get(analysis_id, frfr_info_id)
        if not analysis_record:
            logger.warning(
                f"Analysis record not found: {analysis_id}/{frfr_info_id}"
            )
            return False

        video_update = {
            "video_name": video_name,
            "analysis_status": analysis_status,
        }
        analysis_record["video_updates"].append(video_update)
        return self.update(
            analysis_id,
            frfr_info_id,
            video_updates=analysis_record["video_updates"],
        )

    def remove_video_update(
        self, analysis_id: str, frfr_info_id: str, video_name: str
    ) -> bool:
        """
        분석 레코드에서 비디오 업데이트를 제거합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            video_name: 제거할 비디오 이름

        Returns:
            제거 성공 여부
        """
        analysis_record = self.get(analysis_id, frfr_info_id)
        if not analysis_record:
            logger.warning(
                f"Analysis record not found: {analysis_id}/{frfr_info_id}"
            )
            return False

        original_count = len(analysis_record["video_updates"])
        analysis_record["video_updates"] = [
            v
            for v in analysis_record["video_updates"]
            if v.get("video_name") != video_name
        ]

        if len(analysis_record["video_updates"]) < original_count:
            return self.update(
                analysis_id,
                frfr_info_id,
                video_updates=analysis_record["video_updates"],
            )
        else:
            logger.warning(
                f"Video update not found: {video_name}"
            )
            return False

    def get_all(self) -> List[Dict[str, Any]]:
        """
        모든 분석 정보를 조회합니다.

        Returns:
            분석 정보 리스트
        """
        results = self.table.all()
        logger.info(f"Retrieved {len(results)} total analysis records")
        return results