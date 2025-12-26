from pydantic import BaseModel
from typing import List

class VideoInfo(BaseModel):
    path: str
    name: str
    add_time: str
    analysis_status: str

class FireLocation(BaseModel):
    longitude: float
    latitude: float

class WildfireResponse(BaseModel):
    frfr_info_id: str
    analysis_id: str
    fire_location: FireLocation
    videos: List[VideoInfo]

class WildfireRequest(BaseModel):
    frfr_info_id: str
    analysis_id: str
    
    class Config:
        # 숫자를 문자열로 자동 변환
        str_strip_whitespace = True
        
    @classmethod
    def model_validate(cls, obj):
        # frfr_info_id가 숫자로 들어오면 문자열로 변환
        if isinstance(obj, dict):
            if 'frfr_info_id' in obj and not isinstance(obj['frfr_info_id'], str):
                obj['frfr_info_id'] = str(obj['frfr_info_id'])
        return super().model_validate(obj)