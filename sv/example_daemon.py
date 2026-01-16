import os
import signal
import sys
import time

try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

from datetime import datetime

from sv import LOG_DIR_PATH, PROJECT_ROOT_PATH
from sv.utils.logger import setup_common_logger, setup_logger
from sv.daemon.fline_daemon import FlineDaemon

from sv.task.mock import split_primary_task_result
from sv.task.mock.connection_task import ConnectionTask
from sv.task.mock.video_extract_task import VideoFrameExtractionTask
from sv.task.mock.segmentation_task import VideoSegmentationTask
from sv.task.mock.location_simulation_task import LocationSimulationTask
from sv.task.mock.feature_matching_task import FeatureMatchingTask
from sv.task.mock.geojson_boundary_task import SegmentationGeoJsonTask

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


def main():
    ray.init(num_cpus=8, ignore_reinit_error=True)
    # ray.init(local_mode=True)
    initialize_logger()

    video_request_url = "http://127.0.0.1:8086/wildfire-data-sender/api/wildfire/sender"
    analysis_update_url = "http://127.0.0.1:8086/wildfire-data-receiver/api/wildfire/video-status"
    result_sent_url = "http://127.0.0.1:8086/wildfire-data-receiver/api/wildfire/data"
    result_example = os.path.join(PROJECT_ROOT_PATH, 'data', 'vid', 'cy_all.geojson')

    work_dir = os.path.join(PROJECT_ROOT_PATH, 'data', 'workspace')
    fline_daemon = FlineDaemon(base_work_dir=work_dir,
                               analysis_update_url=analysis_update_url,
                               result_sent_url=result_sent_url)

    fline_daemon.register_primary_task(ConnectionTask(api_url=video_request_url))
    fline_daemon.register_secondary_tasks(
        [VideoFrameExtractionTask(delay_seconds=5), VideoSegmentationTask(delay_seconds=5),
         LocationSimulationTask(delay_seconds=10), FeatureMatchingTask(delay_seconds=20),
         SegmentationGeoJsonTask(result_path=result_example, delay_seconds=5)]
    )
    fline_daemon.set_data_splitter(split_primary_task_result)
    
    # Graceful shutdown 핸들러 설정
    def signal_handler(sig, frame):
        logger.info("=" * 80)
        logger.info("🛑 Shutdown signal received (Ctrl+C)")
        logger.info("=" * 80)
        fline_daemon.stop()
        
        # Ray 종료
        if ray.is_initialized():
            logger.info("Shutting down Ray...")
            ray.shutdown()
        
        logger.info("✅ Daemon stopped gracefully")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 데몬 시작
    fline_daemon.start()
    
    logger.info("=" * 80)
    logger.info("✅ FlineDaemon is running... Press Ctrl+C to stop")
    logger.info("=" * 80)
    
    # 무한 대기 (Ctrl+C 또는 SIGTERM으로 종료)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)


if __name__ == '__main__':
    main()
