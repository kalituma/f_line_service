from enum import Enum
from typing import Optional


class JobExecutionStatus(Enum):
    """Job 실행 상태 enum"""
    SUCCESS = "success"        # 성공
    FAILED = "failed"          # 실패
    NOT_EXISTS = "not_exists"    # 파일 없음

    def to_str(self) -> str:
        """상태를 문자열로 변환"""
        return self.value

    @classmethod
    def from_str(cls, status_str: str) -> Optional['JobExecutionStatus']:
        """문자열을 상태 enum으로 변환"""
        for status in cls:
            if status.value == status_str:
                return status
        return None


# 편의 상수
STATUS_SUCCESS = JobExecutionStatus.SUCCESS
STATUS_FAILED = JobExecutionStatus.FAILED
STATUS_NOT_EXISTS = JobExecutionStatus.NOT_EXISTS