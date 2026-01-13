from typing import Dict, Any, Optional
from datetime import datetime

try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

from sv import LOG_DIR_PATH
from sv.utils.logger import setup_common_logger, setup_logger

from sv.backend.service.app_state_manager import get_app_state_manager, AppState
logger = setup_logger(__name__)

def initialize_logger(log_dir_path=None):
    """
    공통 로거 초기화 (실행 시간을 파일명에 포함)

    Args:
        log_dir_path: 로그 디렉토리 경로 (기본값: sv.LOG_DIR_PATH)

    Returns:
        Path: 생성된 로그 파일 경로
    """
    if log_dir_path is None:
        log_dir_path = LOG_DIR_PATH

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir_path / f"f_line_server_{timestamp}.log"
    setup_common_logger(log_file)

    return log_file

if __name__ == '__main__':
    ray.init(num_cpus=8, ignore_reinit_error=True)

    log_file = initialize_logger()
    app_state = get_app_state_manager()

    logger.info("Starting F-line Server with Daemon...")
    logger.info(f"Log file: {log_file}")
    logger.info(f"App state: {app_state.get_state().value}")

    daemon = FlineDaemon()
