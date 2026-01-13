from enum import Enum
from typing import Optional

class ServerAnalysisStatus(Enum):
    """영상 분석 상태 enum"""
    STAT_001 = "waiting"  # 영상 분석 대기중
    STAT_002 = "analyzing"  # 영상 분석중
    STAT_003 = "fline_extracted"  # 영상 화선 추출 성공
    STAT_004 = "fline_failed"  # 영상 화선 추출 실패
    STAT_005 = "video_receive_failed"  # 영상 수신 실패

    def to_desc_str(self) -> str:
        """Enum의 value(설명)를 문자열로 변환"""
        return self.value

    def to_code(self) -> str:
        """Enum의 name(STAT_001 등)을 문자열로 변환"""
        return self.name

    @classmethod
    def from_desc_str(cls, desc_value: str) -> Optional['ServerAnalysisStatus']:
        """설명 문자열을 Enum으로 변환"""
        for status in cls:
            if status.value == desc_value:
                return status
        return None

    @classmethod
    def from_code(cls, code: str) -> Optional['ServerAnalysisStatus']:
        """상태 코드(STAT_001 등)로 Enum 조회"""
        try:
            return cls[code]
        except KeyError:
            return None

STATUS_WAITING = ServerAnalysisStatus.STAT_001
STATUS_ANALYZING = ServerAnalysisStatus.STAT_002
STATUS_FLINE_EXTRACTED = ServerAnalysisStatus.STAT_003
STATUS_FLINE_FAILED = ServerAnalysisStatus.STAT_004
STATUS_VIDEO_RECEIVE_FAILED = ServerAnalysisStatus.STAT_005
