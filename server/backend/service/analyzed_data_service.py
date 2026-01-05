import json
import logging
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from server.backend.db.table.analyzed_data_table import AnalyzedDataTable
from server.backend.db.database import get_shared_database, SharedDatabase

logger = logging.getLogger(__name__)


class AnalyzedDataRequest(BaseModel):
    """분석 결과 데이터 요청 모델"""
    frfr_info_id: str
    analysis_id: str
    type: str = "FeatureCollection"
    crs: Optional[Dict[str, Any]] = None
    features: List[Dict[str, Any]]
    timestamp: Optional[str] = None


class AnalyzedDataService:
    """분석 결과 데이터를 관리하는 서비스"""

    def __init__(self, db: SharedDatabase = None):
        """
        서비스 초기화

        Args:
            db: SharedDatabase 인스턴스 (선택사항)
        """
        if db is None:
            db = get_shared_database()
        self.db = db
        self.analyzed_data_table = AnalyzedDataTable(db)
        logger.info("AnalyzedDataService initialized")

    def save_analyzed_data(
        self, request_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        GeoJSON 형식의 분석 결과 데이터를 저장합니다.
        데이터가 존재하면 update, 없으면 insert를 수행합니다.

        Args:
            request_data: 다음 구조의 딕셔너리
                {
                    "frfr_info_id": "186525",
                    "analysis_id": "20241225_SEOUL_001",
                    "type": "FeatureCollection",
                    "crs": {
                        "type": "name",
                        "properties": {
                            "name": "EPSG:4326"
                        }
                    },
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {...},
                            "properties": {...}
                        }
                    ],
                    "timestamp": "2024-12-25 13:45:12"  // optional
                }

        Returns:
            저장 결과 정보 딕셔너리
            {
                "success": bool,
                "message": str,
                "record_id": str,
                "operation": "insert" | "update"
            }
        """
        try:
            # Pydantic 모델로 검증
            validated_request = AnalyzedDataRequest(**request_data)

            frfr_info_id = validated_request.frfr_info_id
            analysis_id = validated_request.analysis_id

            logger.info(
                f"Starting save for {frfr_info_id}/{analysis_id}"
            )

            # analysis_id와 frfr_info_id 복합키로 기존 데이터 존재 여부 확인
            existing_data = self.analyzed_data_table.get(
                analysis_id, frfr_info_id
            )

            if existing_data:
                # 기존 데이터가 존재하면 update 수행
                geojson_data = {
                    "type": validated_request.type,
                    "crs": validated_request.crs,
                    "features": validated_request.features,
                }

                update_success = self.update_analyzed_data(
                    analysis_id=analysis_id,
                    frfr_info_id=frfr_info_id,
                    geojson_data=geojson_data,
                    timestamp=validated_request.timestamp,
                )

                if update_success:
                    logger.info(
                        f"Successfully updated analyzed data: "
                        f"{analysis_id}/{frfr_info_id}"
                    )
                    return {
                        "success": True,
                        "message": f"Successfully updated analyzed data for {analysis_id}/{frfr_info_id}",
                        "record_id": f"updated_{analysis_id}_{frfr_info_id}",
                        "operation": "update",
                    }
                else:
                    error_msg = (
                        f"Failed to update analyzed data for "
                        f"{analysis_id}/{frfr_info_id}"
                    )
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "message": error_msg,
                        "record_id": None,
                        "operation": "update",
                    }
            else:
                # 기존 데이터가 없으면 insert 수행
                geojson_data = {
                    "type": validated_request.type,
                    "crs": validated_request.crs,
                    "features": validated_request.features,
                }

                record_id = self.analyzed_data_table.insert(
                    analysis_id=analysis_id,
                    frfr_info_id=frfr_info_id,
                    geojson_data=geojson_data,
                    timestamp=validated_request.timestamp,
                )

                logger.info(
                    f"Successfully inserted analyzed data: "
                    f"{analysis_id}/{frfr_info_id} (record_id: {record_id})"
                )
                return {
                    "success": True,
                    "message": f"Successfully inserted analyzed data for {analysis_id}/{frfr_info_id}",
                    "record_id": record_id,
                    "operation": "insert",
                }

        except Exception as e:
            error_msg = f"Invalid request data: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "message": error_msg,
                "record_id": None,
                "operation": None,
            }

    def save_from_json_string(
        self, json_string: str
    ) -> Dict[str, Any]:
        """
        JSON 문자열에서 분석 결과 데이터를 저장합니다.

        Args:
            json_string: JSON 형식의 문자열

        Returns:
            저장 결과 정보 딕셔너리
        """
        try:
            request_data = json.loads(json_string)
            return self.save_analyzed_data(request_data)
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON format: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "message": error_msg,
                "record_id": None,
                "operation": None,
            }

    def get_analyzed_data(
        self, analysis_id: str, frfr_info_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        특정 분석 결과 데이터를 조회합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            분석 결과 데이터 딕셔너리 또는 None
        """
        return self.analyzed_data_table.get(analysis_id, frfr_info_id)

    def get_analyzed_data_geojson(
        self, analysis_id: str, frfr_info_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        특정 분석 결과 데이터를 GeoJSON 형식으로 조회합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            GeoJSON 형식의 분석 결과 데이터 또는 None
        """
        return self.analyzed_data_table.get_geojson_format(
            analysis_id, frfr_info_id
        )

    def get_all_analyzed_data_by_analysis_id(
        self, analysis_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 분석 ID의 모든 분석 결과 데이터를 조회합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            분석 결과 데이터 리스트
        """
        return self.analyzed_data_table.get_by_analysis_id(analysis_id)

    def get_all_analyzed_data_by_frfr_id(
        self, frfr_info_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 산불 정보 ID의 모든 분석 결과 데이터를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            분석 결과 데이터 리스트
        """
        return self.analyzed_data_table.get_by_frfr_id(frfr_info_id)

    def update_analyzed_data(
        self,
        analysis_id: str,
        frfr_info_id: str,
        geojson_data: Dict[str, Any],
        timestamp: Optional[str] = None,
    ) -> bool:
        """
        분석 결과 데이터를 업데이트합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            geojson_data: GeoJSON 형식의 분석 결과 데이터
            timestamp: 타임스탬프 (선택사항)

        Returns:
            업데이트 성공 여부
        """
        return self.analyzed_data_table.update(
            analysis_id, frfr_info_id, geojson_data, timestamp
        )

    def delete_analyzed_data(
        self, analysis_id: str, frfr_info_id: str
    ) -> bool:
        """
        특정 분석 결과 데이터를 삭제합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        return self.analyzed_data_table.delete(analysis_id, frfr_info_id)

    def delete_by_analysis_id(self, analysis_id: str) -> bool:
        """
        특정 분석 ID의 모든 분석 결과 데이터를 삭제합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            삭제 성공 여부
        """
        return self.analyzed_data_table.delete_by_analysis_id(analysis_id)

    def delete_by_frfr_id(self, frfr_info_id: str) -> bool:
        """
        특정 산불 정보 ID의 모든 분석 결과 데이터를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        return self.analyzed_data_table.delete_by_frfr_id(frfr_info_id)

    def get_all_analyzed_data(self) -> List[Dict[str, Any]]:
        """
        모든 분석 결과 데이터를 조회합니다.

        Returns:
            모든 분석 결과 데이터 리스트
        """
        return self.analyzed_data_table.get_all()

    def check_analyzed_data_exists(
        self, analysis_id: str, frfr_info_id: str
    ) -> bool:
        """
        특정 분석 결과 데이터가 존재하는지 확인합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            데이터 존재 여부 (True: 존재, False: 미존재)
        """
        try:
            logger.info(
                f"Checking if analyzed data exists for "
                f"{analysis_id}/{frfr_info_id}"
            )

            analyzed_data = self.analyzed_data_table.get(
                analysis_id, frfr_info_id
            )

            if not analyzed_data:
                logger.warning(
                    f"No analyzed data found for "
                    f"analysis_id={analysis_id}, frfr_info_id={frfr_info_id}"
                )
                return False

            logger.info(
                f"Analyzed data exists for {analysis_id}/{frfr_info_id}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to check analyzed data existence for "
                f"{analysis_id}/{frfr_info_id}: {str(e)}"
            )
            return False

