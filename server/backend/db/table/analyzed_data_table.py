import logging
from datetime import datetime
from tinydb import Query
from typing import Optional, List, Dict, Any

from server.backend.db.database import get_shared_database, SharedDatabase

logger = logging.getLogger(__name__)


class AnalyzedDataTable:
    """AnalyzedData 테이블 관리 (복합키: analysis_id + frfr_info_id)
    
    GeoJSON 형식의 분석 결과 데이터를 저장 및 관리합니다.
    """

    def __init__(self, db: SharedDatabase = None):
        if db is None:
            db = get_shared_database()
        self.db = db
        self.table = db.get_table("analyzed_data")
        logger.info("AnalyzedDataTable initialized")

    def insert(
        self,
        analysis_id: str,
        frfr_info_id: str,
        geojson_data: Dict[str, Any],
        timestamp: Optional[str] = None,
    ) -> str:
        """
        분석 결과 데이터(GeoJSON)를 저장합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            geojson_data: GeoJSON 형식의 분석 결과 데이터
            timestamp: 타임스탬프 (미제공 시 현재 시간 사용)

        Returns:
            저장된 레코드 ID
        """
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        data = {
            "analysis_id": analysis_id,
            "frfr_info_id": frfr_info_id,
            "timestamp": timestamp,
            "type": geojson_data.get("type"),
            "crs": geojson_data.get("crs"),
            "features": geojson_data.get("features", []),
        }
        doc_id = self.table.insert(data)
        logger.info(
            f"Inserted analyzed data: {analysis_id}/{frfr_info_id}"
        )
        return str(doc_id)

    def get(
        self, analysis_id: str, frfr_info_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        analysis_id와 frfr_info_id 복합키로 분석 결과 데이터를 조회합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            분석 결과 데이터 딕셔너리 또는 None
        """
        data = Query()
        result = self.table.get(
            (data.analysis_id == analysis_id)
            & (data.frfr_info_id == frfr_info_id)
        )
        if result:
            logger.info(
                f"Found analyzed data: {analysis_id}/{frfr_info_id}"
            )
        else:
            logger.warning(
                f"Analyzed data not found: {analysis_id}/{frfr_info_id}"
            )
        return result

    def get_by_analysis_id(
        self, analysis_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 analysis_id의 모든 분석 결과 데이터를 조회합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            분석 결과 데이터 리스트
        """
        data = Query()
        results = self.table.search(data.analysis_id == analysis_id)
        logger.info(
            f"Retrieved {len(results)} analyzed data records for {analysis_id}"
        )
        return results

    def get_by_frfr_id(
        self, frfr_info_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 frfr_info_id의 모든 분석 결과 데이터를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            분석 결과 데이터 리스트
        """
        data = Query()
        results = self.table.search(data.frfr_info_id == frfr_info_id)
        logger.info(
            f"Retrieved {len(results)} analyzed data records for {frfr_info_id}"
        )
        return results

    def update(
        self,
        analysis_id: str,
        frfr_info_id: str,
        geojson_data: Dict[str, Any],
        timestamp: Optional[str] = None,
    ) -> bool:
        """
        분석 결과 데이터를 업데이트합니다.
        analysis_id와 frfr_info_id 복합키로 레코드를 찾아서 업데이트합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            geojson_data: 업데이트할 GeoJSON 형식의 분석 결과 데이터
            timestamp: 타임스탬프 (미제공 시 현재 시간 사용)

        Returns:
            업데이트 성공 여부
        """
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        update_data = {
            "timestamp": timestamp,
            "type": geojson_data.get("type"),
            "crs": geojson_data.get("crs"),
            "features": geojson_data.get("features", []),
        }

        data = Query()
        result = self.table.update(
            update_data,
            (data.analysis_id == analysis_id)
            & (data.frfr_info_id == frfr_info_id),
        )

        if result:
            logger.info(
                f"Updated analyzed data: {analysis_id}/{frfr_info_id}"
            )
            return True
        else:
            logger.warning(
                f"Failed to update analyzed data: "
                f"{analysis_id}/{frfr_info_id}"
            )
            return False

    def delete(
        self, analysis_id: str, frfr_info_id: str
    ) -> bool:
        """
        분석 결과 데이터를 삭제합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        data = Query()
        result = self.table.remove(
            (data.analysis_id == analysis_id)
            & (data.frfr_info_id == frfr_info_id)
        )

        if result:
            logger.info(
                f"Deleted analyzed data: {analysis_id}/{frfr_info_id}"
            )
            return True
        else:
            logger.warning(
                f"Failed to delete analyzed data: "
                f"{analysis_id}/{frfr_info_id}"
            )
            return False

    def delete_by_analysis_id(self, analysis_id: str) -> bool:
        """
        특정 analysis_id의 모든 분석 결과 데이터를 삭제합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            삭제 성공 여부
        """
        data = Query()
        result = self.table.remove(data.analysis_id == analysis_id)

        if result:
            logger.info(
                f"Deleted all analyzed data for {analysis_id}"
            )
            return True
        else:
            logger.warning(
                f"No analyzed data to delete for {analysis_id}"
            )
            return False

    def delete_by_frfr_id(self, frfr_info_id: str) -> bool:
        """
        특정 frfr_info_id의 모든 분석 결과 데이터를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        data = Query()
        result = self.table.remove(data.frfr_info_id == frfr_info_id)

        if result:
            logger.info(
                f"Deleted all analyzed data for {frfr_info_id}"
            )
            return True
        else:
            logger.warning(
                f"No analyzed data to delete for {frfr_info_id}"
            )
            return False

    def get_all(self) -> List[Dict[str, Any]]:
        """
        모든 분석 결과 데이터를 조회합니다.

        Returns:
            분석 결과 데이터 리스트
        """
        results = self.table.all()
        logger.info(f"Retrieved {len(results)} total analyzed data records")
        return results

    def get_geojson_format(
        self, analysis_id: str, frfr_info_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        분석 결과 데이터를 GeoJSON 형식으로 반환합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            GeoJSON 형식의 데이터 또는 None
        """
        result = self.get(analysis_id, frfr_info_id)
        if result:
            return {
                "type": result.get("type", "FeatureCollection"),
                "frfr_info_id": frfr_info_id,
                "analysis_id": analysis_id,
                "timestamp": result.get("timestamp"),
                "crs": result.get("crs"),
                "features": result.get("features", []),
            }
        return None

