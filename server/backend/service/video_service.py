import json
import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel

from server.backend.db.database import get_shared_database, SharedDatabase
from server.backend.db.table.video_table import WildfireVideoTable
from server.utils.config_util import validate_config, load_config_file

logger = logging.getLogger(__name__)

class WildfireVideo(BaseModel):
    """비디오 응답 모델"""
    frfr_info_id: str
    analysis_id: str
    video_name: str
    video_type: str
    video_path: str
    add_time: str

class WildfireVideoService:
    """비디오 설정 파일을 읽어서 DB에 저장하는 서비스"""
    
    def __init__(self, db: SharedDatabase = None):
        if db is None:
            db = get_shared_database()
        self.db = db
        self.wildfire_video_table = WildfireVideoTable(db)
        logger.info("WildfireVideoService initialized")
    
    @staticmethod
    def _extract_video_name(video_path: str) -> str:
        """
        파일 경로에서 비디오 파일명을 추출합니다.
        예: /path/to/FPA630_20230101_ANDONG_1.mp4 -> FPA630_20230101_ANDONG_1.mp4
        """
        return os.path.basename(video_path)
    
    @staticmethod
    def _get_video_creation_time(video_path: str) -> Optional[str]:
        """
        비디오 파일의 생성/수정 시간을 ISO format으로 반환합니다.
        
        Args:
            video_path: 비디오 파일 경로
            
        Returns:
            ISO format 시간 문자열 또는 None
        """
        try:
            if not os.path.exists(video_path):
                logger.warning(f"Cannot get creation time - file not found: {video_path}")
                return None
            
            # 파일의 수정 시간(mtime) 가져오기
            mtime = os.path.getmtime(video_path)
            creation_time = datetime.fromtimestamp(mtime).isoformat()
            logger.debug(f"Got video creation time: {video_path} -> {creation_time}")
            return creation_time
        
        except Exception as e:
            logger.error(f"Failed to get creation time for {video_path}: {str(e)}")
            return None
    
    def insert_from_video_config(self, config_data: Dict[str, Any]) -> None:
        frfr_info_id = config_data["frfr_info_id"]
        video_info_list = config_data["video_info"]
        analysis_id = config_data["analysis_id"]
        total_len = len(video_info_list)

        logger.info(
            f"Starting import for frfr_info_id: {frfr_info_id}, "
            f"analysis_id: {analysis_id}, "
            f"total videos: {total_len}"
        )

        for idx, video_item in enumerate(video_info_list):
            try:
                video_type = video_item["video_type"]
                video_path = video_item["video_path"]
                video_name = self._extract_video_name(video_path)

                # 비디오 경로 존재 여부 확인 (선택사항)
                if not os.path.exists(video_path):
                    logger.warning(
                        f"Video file not found: {video_path} "
                        f"(but continuing with import)"
                    )

                # 타임스탬프 획득
                add_time = self._get_video_creation_time(video_path)

                # 복합키(frfr_info_id, analysis_id, video_name)로 기존 데이터 존재 여부 확인
                existing_video = self.wildfire_video_table.get(
                    frfr_info_id, analysis_id, video_name
                )

                if existing_video:
                    # 기존 데이터가 존재하면 update 수행
                    update_success = self.wildfire_video_table.update(
                        frfr_info_id=frfr_info_id,
                        analysis_id=analysis_id,
                        video_name=video_name,
                        video_type=video_type,
                        video_path=video_path,
                        add_time=add_time
                    )

                    if update_success:
                        logger.info(
                            f"[{idx + 1}/{total_len}] Updated video: "
                            f"{frfr_info_id}/{analysis_id}/{video_name}"
                        )
                    else:
                        logger.warning(
                            f"[{idx + 1}/{total_len}] Failed to update video: "
                            f"{frfr_info_id}/{analysis_id}/{video_name}"
                        )
                else:
                    # 기존 데이터가 없으면 insert 수행
                    doc_id = self.wildfire_video_table.insert(
                        frfr_info_id=frfr_info_id,
                        analysis_id=analysis_id,
                        video_name=video_name,
                        video_type=video_type,
                        video_path=video_path,
                        add_time=add_time
                    )

                    logger.info(
                        f"[{idx + 1}/{total_len}] Inserted video: "
                        f"{frfr_info_id}/{analysis_id}/{video_name} (doc_id: {doc_id})"
                    )
            except Exception as e:
                error_msg = (
                    f"Failed to import video[{idx}] "
                    f"({video_item.get('video_path', 'unknown')}): {str(e)}"
                )
                logger.error(error_msg)
    
    def get_videos_by_frfr_id(self, frfr_info_id: str) -> List[WildfireVideo]:
        """
        특정 frfr_info_id의 모든 비디오 정보를 조회합니다.
        
        Args:
            frfr_info_id: 산불 정보 ID
            
        Returns:
            비디오 정보 리스트
        """
        records = self.wildfire_video_table.get_by_frfr_id(frfr_info_id)
        return [WildfireVideo(**record) for record in records]

    def get_videos_by_frfr_id_and_analysis_id(
        self, frfr_info_id: str, analysis_id: str
    ) -> List[WildfireVideo]:
        """
        특정 frfr_info_id와 analysis_id의 모든 비디오 정보를 조회합니다.
        
        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            
        Returns:
            비디오 정보 리스트
        """
        records = self.wildfire_video_table.get_by_frfr_id_and_analysis_id(
            frfr_info_id, analysis_id
        )
        return [WildfireVideo(**record) for record in records]
    
    def get_videos_by_type(self, frfr_info_id: str, video_type: str) -> List[WildfireVideo]:
        """
        특정 frfr_info_id와 video_type의 비디오 정보를 조회합니다.
        
        Args:
            frfr_info_id: 산불 정보 ID
            video_type: 비디오 타입 (예: FPA630, FPA601)
            
        Returns:
            비디오 정보 리스트
        """
        all_videos = self.wildfire_video_table.get_by_frfr_id(frfr_info_id)
        filtered_videos = [
            record for record in all_videos
            if record.get("video_type") == video_type
        ]
        return [WildfireVideo(**record) for record in filtered_videos]

    def get_videos_by_type_and_analysis_id(
        self, frfr_info_id: str, analysis_id: str, video_type: str
    ) -> List[WildfireVideo]:
        """
        특정 frfr_info_id, analysis_id와 video_type의 비디오 정보를 조회합니다.
        
        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            video_type: 비디오 타입 (예: FPA630, FPA601)
            
        Returns:
            비디오 정보 리스트
        """
        all_videos = self.wildfire_video_table.get_by_frfr_id_and_analysis_id(
            frfr_info_id, analysis_id
        )
        filtered_videos = [
            record for record in all_videos
            if record.get("video_type") == video_type
        ]
        return [WildfireVideo(**record) for record in filtered_videos]
    
    def get_all_videos(self) -> List[WildfireVideo]:
        """
        모든 비디오 정보를 조회합니다.
        
        Returns:
            비디오 정보 리스트
        """
        records = self.wildfire_video_table.get_all()
        return [WildfireVideo(**record) for record in records]
    
    def get_video_paths_by_frfr_id(self, frfr_info_id: str) -> List[Dict[str, str]]:
        """
        특정 frfr_info_id의 video_name과 video_path를 딕셔너리 리스트로 반환합니다.
        
        Args:
            frfr_info_id: 산불 정보 ID
            
        Returns:
            [{"video_name": str, "video_path": str}, ...] 형태의 리스트
        """
        videos = self.get_videos_by_frfr_id(frfr_info_id)
        return [
            {
                "video_name": video.video_name,
                "video_path": video.video_path
            }
            for video in videos
        ]

    def get_video_paths_by_frfr_id_and_analysis_id(
        self, frfr_info_id: str, analysis_id: str
    ) -> List[Dict[str, str]]:
        """
        특정 frfr_info_id와 analysis_id의 video_name과 video_path를 딕셔너리 리스트로 반환합니다.
        
        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            
        Returns:
            [{"video_name": str, "video_path": str}, ...] 형태의 리스트
        """
        videos = self.get_videos_by_frfr_id_and_analysis_id(frfr_info_id, analysis_id)
        return [
            {
                "video_name": video.video_name,
                "video_path": video.video_path
            }
            for video in videos
        ]
    
    def get_video_paths_by_type(self, frfr_info_id: str, video_type: str) -> List[Dict[str, str]]:
        """
        특정 frfr_info_id와 video_type의 video_name과 video_path를 딕셔너리 리스트로 반환합니다.
        
        Args:
            frfr_info_id: 산불 정보 ID
            video_type: 비디오 타입 (예: FPA630, FPA601)
            
        Returns:
            [{"video_name": str, "video_path": str}, ...] 형태의 리스트
        """
        videos = self.get_videos_by_type(frfr_info_id, video_type)
        return [
            {
                "video_name": video.video_name,
                "video_path": video.video_path
            }
            for video in videos
        ]

    def get_video_paths_by_type_and_analysis_id(
        self, frfr_info_id: str, analysis_id: str, video_type: str
    ) -> List[Dict[str, str]]:
        """
        특정 frfr_info_id, analysis_id와 video_type의 video_name과 video_path를 딕셔너리 리스트로 반환합니다.
        
        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            video_type: 비디오 타입 (예: FPA630, FPA601)
            
        Returns:
            [{"video_name": str, "video_path": str}, ...] 형태의 리스트
        """
        videos = self.get_videos_by_type_and_analysis_id(
            frfr_info_id, analysis_id, video_type
        )
        return [
            {
                "video_name": video.video_name,
                "video_path": video.video_path
            }
            for video in videos
        ]
    
    def get_all_video_paths(self) -> List[Dict[str, str]]:
        """
        모든 비디오의 video_name과 video_path를 딕셔너리 리스트로 반환합니다.
        
        Returns:
            [{"video_name": str, "video_path": str}, ...] 형태의 리스트
        """
        videos = self.get_all_videos()
        return [
            {
                "video_name": video.video_name,
                "video_path": video.video_path
            }
            for video in videos
        ]

    def check_video_exists_by_name(
        self, frfr_info_id: str, video_name: str
    ) -> bool:
        """
        특정 frfr_info_id와 video_name을 가진 비디오가 존재하는지 확인합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            video_name: 비디오 이름

        Returns:
            데이터 존재 여부 (True: 존재, False: 미존재)
        """
        try:
            logger.info(
                f"Checking if video exists: frfr_info_id={frfr_info_id}, "
                f"video_name={video_name}"
            )
            
            videos = self.get_videos_by_frfr_id(frfr_info_id)
            
            for video in videos:
                if video.video_name == video_name:
                    logger.info(
                        f"Video found: frfr_info_id={frfr_info_id}, "
                        f"video_name={video_name}"
                    )
                    return True
            
            logger.warning(
                f"Video not found: frfr_info_id={frfr_info_id}, "
                f"video_name={video_name}"
            )
            return False
            
        except Exception as e:
            logger.error(
                f"Failed to check video existence for "
                f"frfr_info_id={frfr_info_id}, video_name={video_name}: "
                f"{str(e)}"
            )
            return False

    def check_video_exists_by_name_and_analysis_id(
        self, frfr_info_id: str, analysis_id: str, video_name: str
    ) -> bool:
        """
        특정 frfr_info_id, analysis_id와 video_name을 가진 비디오가 존재하는지 확인합니다.

        Args:
            frfr_info_id: 산불 정보 ID
            analysis_id: 분석 ID
            video_name: 비디오 이름

        Returns:
            데이터 존재 여부 (True: 존재, False: 미존재)
        """
        try:
            logger.info(
                f"Checking if video exists: frfr_info_id={frfr_info_id}, "
                f"analysis_id={analysis_id}, video_name={video_name}"
            )
            
            videos = self.get_videos_by_frfr_id_and_analysis_id(
                frfr_info_id, analysis_id
            )
            
            for video in videos:
                if video.video_name == video_name:
                    logger.info(
                        f"Video found: frfr_info_id={frfr_info_id}, "
                        f"analysis_id={analysis_id}, video_name={video_name}"
                    )
                    return True
            
            logger.warning(
                f"Video not found: frfr_info_id={frfr_info_id}, "
                f"analysis_id={analysis_id}, video_name={video_name}"
            )
            return False
            
        except Exception as e:
            logger.error(
                f"Failed to check video existence for "
                f"frfr_info_id={frfr_info_id}, analysis_id={analysis_id}, "
                f"video_name={video_name}: {str(e)}"
            )
            return False
