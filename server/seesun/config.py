"""
분석 전송 스케줄러 서비스 설정값.
"""
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """환경 변수와 기본값을 통해 스케줄러 설정을 관리한다."""
    # 애플리케이션
    app_name: str = "wildfire-analysis-dispatcher"
    log_level: str = "INFO"

    # 데이터베이스
    db_host: str = "wildfire-postgis"
    db_port: int = 5432
    db_name: str = "geoserver"
    db_user: str = "post132gres"
    db_password: str = "se7cret!8A5c"

    # 스케줄링
    poll_interval_seconds: int = 30
    dispatch_interval_minutes: int = 25

    # AI 모델 엔드포인트 (HTTP 사용 시에만 필요, 내부 함수 호출이면 비워두기)
    ai_dispatch_url: Optional[str] = None
    ai_auth_token: Optional[str] = None
    request_timeout_seconds: int = 10
    mark_group002_on_dispatch: bool = True
    backoff_on_failure: bool = True

    # 로컬 상태 파일
    state_file: str = "./wildfire-analysis-dispatcher/state/dispatched.json"

    class Config:
        env_file = ".env"
        case_sensitive = False

    @property
    def dispatch_interval_seconds(self) -> int:
        """분 단위 전송 간격을 초 단위로 변환한다."""
        return self.dispatch_interval_minutes * 60


settings = Settings()

