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
        self.table = db.get_table("wildfire_info")
        logger.info("WildfireTable initialized")

    def insert(
        self,
        frfr_info_id: str,
        fire_location: Dict[str, float],
    ) -> str:
        """
        산불 정보를 데이터베이스에 저장합니다.

        Args:
            frfr_info_id: 산불 정보 ID (Primary Key)
            fire_location: 위치 정보 {'latitude': float, 'longitude': float}

        Returns:
            저장된 레코드 ID
        """
        data = {
            "frfr_info_id": frfr_info_id,
            "fire_location": fire_location,
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
    ) -> bool:
        """
        산불 정보를 업데이트합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            fire_location: 업데이트할 위치 정보

        Returns:
            업데이트 성공 여부
        """
        wildfire = Query()
        update_data = {}

        if fire_location:
            update_data["fire_location"] = fire_location

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