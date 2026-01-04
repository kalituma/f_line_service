from pydantic import BaseModel
import json
import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime

from server.backend.db.table.wildfire_table import WildfireTable
from server.backend.db.database import get_shared_database, SharedDatabase


class WildFireLocation(BaseModel):
    latitude: float
    longitude: float

class FireLocationRequest(BaseModel):
    """산불 위치 정보 요청 모델"""
    frfr_info_id: str
    location: WildFireLocation

logger = logging.getLogger(__name__)


class FireInfoService:
    """산불 정보를 관리하는 서비스"""

    def __init__(self, db: SharedDatabase = None):
        """
        서비스 초기화

        Args:
            db: SharedDatabase 인스턴스 (선택사항)
        """
        if db is None:
            db = get_shared_database()
        self.db = db
        self.wildfire_table = WildfireTable(db)
        logger.info("FireInfoService initialized")

    def save_fire_location(
        self, request_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        JSON 형식의 산불 위치 데이터를 저장합니다.

        Args:
            request_data: 다음 구조의 딕셔너리
                {
                    "frfr_info_id": "123456",
                    "location": {
                        "latitude": 36.30740755588261,
                        "longitude": 128.45275734365313
                    }
                }

        Returns:
            저장 결과 정보 딕셔너리
            {
                "success": bool,
                "record_id": str,
                "frfr_info_id": str,
                "error": str (실패 시에만)
            }
        """
        try:
            # Pydantic 모델로 검증
            validated_request = FireLocationRequest(**request_data)

            frfr_info_id = validated_request.frfr_info_id
            fire_location = {
                "latitude": validated_request.location.latitude,
                "longitude": validated_request.location.longitude,
            }

            logger.info(
                f"Saving fire location for {frfr_info_id}: "
                f"({fire_location['latitude']}, {fire_location['longitude']})"
            )

            # 데이터베이스에 저장
            record_id = self.wildfire_table.insert(
                frfr_info_id=frfr_info_id,
                fire_location=fire_location,
            )

            result = {
                "success": True,
                "record_id": record_id,
                "frfr_info_id": frfr_info_id,
                "fire_location": fire_location,
            }

            logger.info(f"Successfully saved fire location: {frfr_info_id}")
            return result

        except Exception as e:
            error_msg = f"Failed to save fire location: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "frfr_info_id": request_data.get("frfr_info_id", "unknown"),
            }

    def save_from_json_string(self, json_string: str) -> Dict[str, Any]:
        """
        JSON 문자열에서 산불 위치 데이터를 저장합니다.

        Args:
            json_string: JSON 형식의 문자열

        Returns:
            저장 결과 정보 딕셔너리
        """
        try:
            request_data = json.loads(json_string)
            return self.save_fire_location(request_data)
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON format: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
            }

    def get_fire_location(self, frfr_info_id: str) -> Optional[Dict[str, Any]]:
        """
        특정 산불 위치 정보를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            산불 정보 딕셔너리 또는 None
        """
        return self.wildfire_table.get(frfr_info_id)

    def update_fire_location(
        self, frfr_info_id: str, latitude: float, longitude: float
    ) -> bool:
        """
        산불 위치 정보를 업데이트합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            latitude: 위도
            longitude: 경도

        Returns:
            업데이트 성공 여부
        """
        fire_location = {
            "latitude": latitude,
            "longitude": longitude,
        }
        return self.wildfire_table.update(frfr_info_id, fire_location)

    def delete_fire_location(self, frfr_info_id: str) -> bool:
        """
        산불 위치 정보를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        return self.wildfire_table.delete(frfr_info_id)

    def get_all_fire_locations(self) -> List[Dict[str, Any]]:
        """
        모든 산불 위치 정보를 조회합니다.

        Returns:
            모든 산불 정보 리스트
        """
        return self.wildfire_table.get_all()
