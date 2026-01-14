from pydantic import BaseModel, field_validator
from typing import List, Optional

class WildfireVideoRequest(BaseModel):
    frfr_info_id: str
    analysis_id: str

    @field_validator('frfr_info_id', mode='before')
    @classmethod
    def convert_frfr_info_id_to_str(cls, v):
        """frfr_info_id를 문자열로 변환"""
        return str(v)

class VideoStatusUpdate(BaseModel):
    """비디오 상태 업데이트 항목"""
    video_name: str
    analysis_status: str

class VideoStatusUpdateRequest(BaseModel):
    """비디오 분석 상태 업데이트 요청"""
    frfr_info_id: str
    analysis_id: str
    video_updates: List[VideoStatusUpdate]
    
    @field_validator('frfr_info_id', mode='before')
    @classmethod
    def convert_frfr_info_id_to_str(cls, v):
        """frfr_info_id를 문자열로 변환"""
        return str(v)

class WildfireInfoPair(BaseModel):
    """산불 정보와 분석 ID 쌍"""
    frfr_info_id: str
    analysis_id: str

class WildfireInfoListResponse(BaseModel):
    """저장된 산불 정보 목록 응답"""
    total_count: int
    data: List[WildfireInfoPair]