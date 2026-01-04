import json
import logging
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from server.backend.db.table.analysis_status_table import AnalysisStatusTable
from server.backend.db.database import get_shared_database, SharedDatabase

logger = logging.getLogger(__name__)


class VideoUpdate(BaseModel):
    """비디오 분석 상태 업데이트 정보"""
    video_name: str
    analysis_status: str


class AnalysisStatusRequest(BaseModel):
    """분석 상태 요청 모델"""
    frfr_info_id: str
    analysis_id: str
    video_updates: List[VideoUpdate]


class AnalysisStatusService:
    """분석 상태 정보를 관리하는 서비스"""

    def __init__(self, db: SharedDatabase = None):
        """
        서비스 초기화

        Args:
            db: SharedDatabase 인스턴스 (선택사항)
        """
        if db is None:
            db = get_shared_database()
        self.db = db
        self.analysis_status_table = AnalysisStatusTable(db)
        logger.info("AnalysisStatusService initialized")

    def save_analysis_status_batch(
        self, request_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        JSON 형식의 분석 상태 데이터를 배치로 저장합니다.

        Args:
            request_data: 다음 구조의 딕셔너리
                {
                    "frfr_info_id": "186525",
                    "analysis_id": "20241225_SEOUL_001",
                    "video_updates": [
                        {
                            "video_name": "FPA702_15-25-00_15-30-00.mp4",
                            "analysis_status": "STAT_003"
                        },
                        ...
                    ]
                }

        Returns:
            저장 결과 정보 딕셔너리
            {
                "success": bool,
                "total_count": int,
                "saved_count": int,
                "failed_count": int,
                "errors": list,
                "record_ids": list
            }
        """
        try:
            # Pydantic 모델로 검증
            validated_request = AnalysisStatusRequest(**request_data)

            frfr_info_id = validated_request.frfr_info_id
            analysis_id = validated_request.analysis_id
            video_updates = validated_request.video_updates

            total_count = len(video_updates)
            saved_count = 0
            failed_count = 0
            errors = []
            record_ids = []

            logger.info(
                f"Starting batch save for {frfr_info_id}/{analysis_id} "
                f"with {total_count} video updates"
            )

            # 각 비디오 업데이트를 처리
            for idx, video_update in enumerate(video_updates):
                try:
                    record_id = self.analysis_status_table.insert(
                        analysis_id=analysis_id,
                        frfr_info_id=frfr_info_id,
                        video_name=video_update.video_name,
                        analysis_status=video_update.analysis_status,
                    )
                    record_ids.append(record_id)
                    saved_count += 1
                    logger.debug(
                        f"Successfully saved record {idx + 1}/{total_count}: "
                        f"{video_update.video_name} -> {video_update.analysis_status}"
                    )
                except Exception as e:
                    failed_count += 1
                    error_msg = (
                        f"Failed to save record {idx + 1}/{total_count}: "
                        f"{str(e)}"
                    )
                    errors.append(error_msg)
                    logger.error(error_msg)

            result = {
                "success": failed_count == 0,
                "total_count": total_count,
                "saved_count": saved_count,
                "failed_count": failed_count,
                "errors": errors,
                "record_ids": record_ids,
            }

            logger.info(
                f"Batch save completed: saved={saved_count}, "
                f"failed={failed_count} out of {total_count}"
            )

            return result

        except Exception as e:
            error_msg = f"Invalid request data: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "total_count": 0,
                "saved_count": 0,
                "failed_count": 0,
                "errors": [error_msg],
                "record_ids": [],
            }

    def save_from_json_string(
        self, json_string: str
    ) -> Dict[str, Any]:
        """
        JSON 문자열에서 분석 상태 데이터를 저장합니다.

        Args:
            json_string: JSON 형식의 문자열

        Returns:
            저장 결과 정보 딕셔너리
        """
        try:
            request_data = json.loads(json_string)
            return self.save_analysis_status_batch(request_data)
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON format: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "total_count": 0,
                "saved_count": 0,
                "failed_count": 0,
                "errors": [error_msg],
                "record_ids": [],
            }

    def get_analysis_status(
        self, analysis_id: str, frfr_info_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        특정 분석 정보를 조회합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            분석 정보 딕셔너리 또는 None
        """
        return self.analysis_status_table.get(analysis_id, frfr_info_id)

    def get_all_status_by_analysis_id(
        self, analysis_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 분석 ID의 모든 상태 정보를 조회합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            분석 상태 정보 리스트
        """
        return self.analysis_status_table.get_by_analysis_id(analysis_id)

    def get_all_status_by_frfr_id(
        self, frfr_info_id: str
    ) -> List[Dict[str, Any]]:
        """
        특정 산불 정보 ID의 모든 상태 정보를 조회합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            분석 상태 정보 리스트
        """
        return self.analysis_status_table.get_by_frfr_id(frfr_info_id)

    def update_analysis_status(
        self,
        analysis_id: str,
        frfr_info_id: str,
        video_name: str = None,
        analysis_status: str = None,
    ) -> bool:
        """
        분석 상태 정보를 업데이트합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID
            video_name: 비디오 이름 (선택사항)
            analysis_status: 분석 상태 (선택사항)

        Returns:
            업데이트 성공 여부
        """
        return self.analysis_status_table.update(
            analysis_id, frfr_info_id, video_name, analysis_status
        )

    def delete_analysis_status(
        self, analysis_id: str, frfr_info_id: str
    ) -> bool:
        """
        특정 분석 상태 정보를 삭제합니다.

        Args:
            analysis_id: 분석 ID
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        return self.analysis_status_table.delete(analysis_id, frfr_info_id)

    def delete_by_analysis_id(self, analysis_id: str) -> bool:
        """
        특정 분석 ID의 모든 상태 정보를 삭제합니다.

        Args:
            analysis_id: 분석 ID

        Returns:
            삭제 성공 여부
        """
        return self.analysis_status_table.delete_by_analysis_id(analysis_id)

    def delete_by_frfr_id(self, frfr_info_id: str) -> bool:
        """
        특정 산불 정보 ID의 모든 상태 정보를 삭제합니다.

        Args:
            frfr_info_id: 산불 정보 ID

        Returns:
            삭제 성공 여부
        """
        return self.analysis_status_table.delete_by_frfr_id(frfr_info_id)

    def get_all_analysis_status(self) -> List[Dict[str, Any]]:
        """
        모든 분석 상태 정보를 조회합니다.

        Returns:
            모든 분석 상태 정보 리스트
        """
        return self.analysis_status_table.get_all()

