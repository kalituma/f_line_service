"""
공통 로거 설정 유틸리티

모든 모듈에서 StreamHandler와 공통 FileHandler를 동일하게 설정합니다.
"""

import logging
from pathlib import Path


def setup_common_logger(log_file: Path | None = None, level: int = logging.INFO) -> None:
    """
    프로젝트 전체에서 사용할 공통 로거를 초기화합니다.
    
    서버 시작 시 main 함수에서 한 번만 호출되어야 합니다.
    
    Args:
        log_file: 공통 로그 파일 경로 (None이면 콘솔 출력만)
        level: 로깅 레벨 (기본값: logging.INFO)        
    """
    import sv
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 기존 핸들러 제거
    root_logger.handlers.clear()
    
    # 포맷 설정
    format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(format_string)
    
    # StreamHandler (콘솔)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)
    
    # FileHandler (공통 파일) - log_file이 제공된 경우만
    if log_file is not None:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # sv 패키지의 COMMON_LOG_FILE 설정
        sv.COMMON_LOG_FILE = log_file
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def setup_logger(
    name: str,
    level: int = logging.INFO
) -> logging.Logger:
    """
    개별 모듈의 로거를 설정합니다.
    
    setup_common_logger()로 초기화된 공통 로거를 사용합니다.
    StreamHandler (콘솔)과 공통 FileHandler (파일) 모두에 출력됩니다.
    
    Args:
        name: 로거 이름 (일반적으로 __name__)
        level: 로깅 레벨 (기본값: logging.INFO)
        
    Returns:
        설정된 Logger 인스턴스        
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    return logger

