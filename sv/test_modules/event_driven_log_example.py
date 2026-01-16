try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

from typing import Optional, Dict, Any
from threading import Thread, Event as ThreadEvent
from datetime import datetime
import time

import os
from sv import PROJECT_ROOT_PATH
from sv.daemon.module.db_change_listener import DBChangeListener, DBChangeEvent, ChangeEventType
from sv.daemon.module.execution_engine import ExecutionEngine
from sv.daemon.module.work_manager import WorkManager
from sv.daemon.module.task_manager import TaskManager
from sv.utils.logger import setup_logger
from sv.backend.service.service_manager import get_service_manager
from sv.daemon.module.update_handler import send_video_status_update
from sv.daemon.module.http_request_client import post_request, HttpRequestError
from sv.daemon.server_state import ServerAnalysisStatus
from sv.daemon.daemon_state import STATUS_FAILED, STATUS_SUCCESS, STATUS_NOT_EXISTS

from sv.test_modules.test_tasks import split_primary_task_result
from sv.task.mock.connection_task import ConnectionTask
from sv.task.mock.video_extract_task import VideoFrameExtractionTask
from sv.task.mock.segmentation_task import VideoSegmentationTask
from sv.task.mock.location_simulation_task import LocationSimulationTask
from sv.task.mock.feature_matching_task import FeatureMatchingTask
from sv.task.mock.geojson_boundary_task import SegmentationGeoJsonTask

logger = setup_logger(__name__)


def initialize_logger() -> None:
    setup_common_logger(None)


service_manager = get_service_manager()
if not service_manager.is_initialized():
    logger.info("ServiceManager 초기화 중...")
    service_manager.initialize_all_services()


class EventDrivenLogger:
    def __init__(
            self,
            poll_interval: float = 2.0,
            num_executors: int = 2,
            run_once: bool = False,
    ):
        self.video_request_url = "http://127.0.0.1:8086/wildfire-data-sender/api/wildfire/sender"
        self.analysis_update_url = "http://127.0.0.1:8086/wildfire-data-receiver/api/wildfire/video-status"
        self.result_sent_url = "http://127.0.0.1:8086/wildfire-data-receiver/api/wildfire/data"
        self.work_dir = os.path.join(PROJECT_ROOT_PATH, 'data', 'workspace')
        self.result_path = os.path.join(PROJECT_ROOT_PATH, 'data', 'vid', 'cy_all.geojson')

        self.work_manager = WorkManager()
        self.task_manager = TaskManager()

        self.task_manager.register_primary_task(ConnectionTask(api_url=self.video_request_url))
        self.task_manager.register_secondary_tasks(
            [VideoFrameExtractionTask(delay_seconds=5), VideoSegmentationTask(delay_seconds=5),
             LocationSimulationTask(delay_seconds=10), FeatureMatchingTask(delay_seconds=20),
             SegmentationGeoJsonTask(result_path=self.result_path, delay_seconds=5)])
        self.task_manager.set_data_splitter(split_primary_task_result)

        self.execution_engine = ExecutionEngine(base_work_dir=self.work_dir, update_url=self.analysis_update_url,
                                                num_executors=num_executors)

        self.db_listener = DBChangeListener(poll_interval)
        self.db_listener.on_change(self._handle_db_change)
        self.running = False
        self.run_once = run_once  # 1회 실행 모드

        self.listener_thread: Optional[Thread] = None
        self.monitor_thread: Optional[Thread] = None

        self.poll_interval = poll_interval
        self.stop_event = ThreadEvent()

    ############################################## event processor ##############################################

    def _handle_db_change(self, event: DBChangeEvent) -> None:
        """
        DB 변경 이벤트 내부 핸들러
        
        Args:
            event: DB 변경 이벤트
        """
        logger.info(f"DB Change detected: {event}")

        # Work Queue INSERT 이벤트 처리
        if event.table_name == "work_queue" and event.event_type == ChangeEventType.PENDING_WORKS_DETECTED:
            logger.info(f"✓ New work detected: {event.data}")
            if self._on_work_created:
                try:
                    self._on_work_created()
                except Exception as e:
                    logger.error(f"Error in on_work_created callback: {str(e)}", exc_info=True)

    def check_changes(self) -> None:
        """
        Pending 상태의 Work 개수 확인
        """
        try:
            self.db_listener.check_pending_works()
        except Exception as e:
            logger.error(f"Error checking pending works: {str(e)}")

    #########################################################################################################

    def _on_work_created(self) -> None:
        """Work 생성 감지 시 핸들러 (Event Processor에서 호출)"""
        logger.info("Work creation event detected")
        self._process_pending_work()

    def _on_work_complete(self, work_id: str, result: Dict[str, Any]) -> None:
        """Work 완료 시 핸들러 (Event Processor에서 호출)"""
        logger.info(f"Work {work_id} completed")

        # Work 상태에 따라 처리
        status = result.get('status')
        if status == 'failed':
            error_msg = result.get('error', 'Unknown error')
            logger.error(f"Work {work_id} failed: {error_msg}")
        else:
            self._update_video_status(work_id, result)
            self._send_analysis_results(work_id, result)

    def _update_video_status(self, work_id: str, result: Dict[str, Any]) -> None:
        """
        Work 상태를 서버에 업데이트
        
        Args:
            work_id: Work ID
            result: Work의 결과 정보 (status 포함)
        """
        try:
            loop_context = result.get('loop_context')
            frfr_id = loop_context.get('frfr_id')
            analysis_id = loop_context.get('analysis_id')

            if not frfr_id or not analysis_id:
                logger.error(f"❌ Missing frfr_id or analysis_id for work {work_id}")
                return

            video_updates = []
            videos_results = result.get('item_results')
            for video_result in videos_results:
                video = video_result.get('data_item')
                video_name = video.get('video_name')
                status = video_result.get('status')
                if status == STATUS_SUCCESS.to_str():
                    status_code = ServerAnalysisStatus.STAT_003.to_code()  # fline_extracted
                elif status == STATUS_FAILED.to_str():
                    status_code = ServerAnalysisStatus.STAT_004.to_code()  # fline_failed
                elif status == STATUS_NOT_EXISTS.to_str():
                    status_code = ServerAnalysisStatus.STAT_005.to_code()  # video_receive_failed
                else:
                    logger.error(f"❌ Unknown status for video {video_name}: {status}")
                    continue
                video_updates.append({
                    "video_name": video_name,
                    "analysis_status": status_code
                })

            # 상태 업데이트
            logger.info(f"📤 Sending work status for work {work_id} to server...")

            send_video_status_update(
                update_url=self.analysis_update_url,
                frfr_id=frfr_id,
                analysis_id=analysis_id,
                video_updates=video_updates
            )

            logger.info(f"✅ Work {work_id} status sent to server")

        except Exception as e:
            logger.error(f"❌ Error updating work status for {work_id}: {str(e)}", exc_info=True)

    def _send_analysis_results(self, work_id: str, result: Dict[str, Any]) -> None:
        """
        성공한 작업의 SegmentationGeoJsonTask 결과를 서버로 전송
        
        Args:
            work_id: Work ID
            result: Work의 결과 정보
        """
        try:
            loop_context = result.get('loop_context')
            frfr_id = loop_context.get('frfr_id')
            analysis_id = loop_context.get('analysis_id')
            
            if not frfr_id or not analysis_id:
                logger.error(f"❌ Missing frfr_id or analysis_id for work {work_id}")
                return
            
            # item_results에서 성공한 항목만 처리
            videos_results = result.get('item_results', [])
            sent_count = 0
            
            for video_result in videos_results:
                status = video_result.get('status')
                
                # status가 'success'인 경우만 처리
                if status != STATUS_SUCCESS.to_str():
                    continue
                
                # tasks에서 SegmentationGeoJsonTask 결과 찾기
                tasks = video_result.get('tasks', [])
                segmentation_task_result = None
                
                for task in tasks:
                    if task.get('task_name') == 'SegmentationGeoJsonTask':                        
                        segmentation_task_result = task.get('result')
                        break
                
                if not segmentation_task_result:
                    logger.warning("⚠️  No SegmentationGeoJsonTask result found for video")
                    continue
                
                # integrated_convex_hull 가져오기
                integrated_convex_hull = segmentation_task_result.get('integrated_convex_hull')
                
                if not integrated_convex_hull:
                    logger.warning("⚠️  No integrated_convex_hull found in SegmentationGeoJsonTask result")
                    continue
                
                # GeoJSON에 메타데이터 추가
                enriched_geojson = integrated_convex_hull.copy()
                
                # frfr_id를 frfr_info_id로 추가
                enriched_geojson['frfr_info_id'] = frfr_id
                
                # analysis_id 추가
                enriched_geojson['analysis_id'] = analysis_id
                
                # 현재 시간을 yyyy-MM-dd HH:mm:ss 형태로 추가
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                enriched_geojson['timestamp'] = current_time
                
                # 서버로 POST 전송
                try:
                    logger.info(f"📤 Sending analysis result to {self.result_sent_url}...")
                    logger.debug(f"   frfr_info_id: {frfr_id}")
                    logger.debug(f"   analysis_id: {analysis_id}")
                    logger.debug(f"   timestamp: {current_time}")
                    
                    response = post_request(
                        url=self.result_sent_url,
                        json_data=enriched_geojson,
                        headers={'Content-Type': 'application/json'},
                        timeout=30
                    )
                    
                    logger.info("✅ Analysis result sent successfully")
                    logger.debug(f"   Response: {response.text}")
                    sent_count += 1
                        
                except HttpRequestError as e:
                    logger.error(f"❌ HTTP request error sending analysis result: {str(e)}")
                except Exception as e:
                    logger.error(f"❌ Error sending analysis result: {str(e)}", exc_info=True)
            
            if sent_count > 0:
                logger.info(f"✅ Sent {sent_count} analysis result(s) for work {work_id}")
            else:
                logger.info(f"ℹ️  No analysis results to send for work {work_id}")
                
        except Exception as e:
            logger.error(f"❌ Error in _send_analysis_results for {work_id}: {str(e)}", exc_info=True)

    def _process_pending_work(self) -> None:
        """
        대기 중인 Work 처리 (비동기 콜백 방식)
        
        Flow:
        1. 다음 PENDING Work 가져오기
        2. Task 실행 (논블로킹)
        3. 완료 시 _on_work_complete 콜백 호출
        """
        work_info = self.work_manager.get_next_pending_work()

        if not work_info:
            logger.debug("No pending works")
            return

        work_id = work_info['work_id']
        frfr_id = work_info['frfr_id']

        # Task 실행 준비 확인
        if not self.task_manager.are_tasks_ready():
            logger.error("❌ Tasks not ready for execution")
            return

        # Task 실행 (논블로킹 - 콜백 방식)
        self.execution_engine.execute_work(
            work_info=work_info,
            primary_task=self.task_manager.primary_task,
            secondary_tasks=self.task_manager.secondary_tasks,
            data_splitter=self.task_manager.data_splitter,
            on_job_complete=self._on_work_complete  # 콜백 함수
        )

        logger.info(f"✓ Work {work_id} (frfr_id={frfr_id}) submitted")

    def _listener_thread_func(self):
        """DB 변경 감지 리스너 스레드"""
        if self.run_once:
            logger.info("🎧 DB Change Listener started (run_once mode)")
        else:
            logger.info("🎧 DB Change Listener started (continuous mode)")

        while not self.stop_event.is_set():
            try:
                # Work Queue 테이블 감시 (PENDING 상태)
                self.check_changes()

                # run_once 모드면 1번만 실행하고 종료
                if self.run_once:
                    logger.info("✓ Run once mode: completed one check cycle")
                    break

                time.sleep(self.poll_interval)

            except Exception as e:
                logger.error(f"Error in listener thread: {str(e)}", exc_info=True)

                # run_once 모드에서도 에러 발생 시 종료
                if self.run_once:
                    break

                time.sleep(self.poll_interval)

        logger.info("🎧 DB Change Listener stopped")

    def _ray_monitor_thread_func(self) -> None:
        """Ray Work 모니터 스레드 (ExecutionEngine의 pending jobs 모니터링)"""
        logger.info("⚡ Ray Work Monitor started")

        while not self.stop_event.is_set():
            try:
                if not self.execution_engine:
                    time.sleep(self.poll_interval)
                    continue

                # ExecutionEngine에서 완료된 작업 확인 및 처리
                completed_jobs = self.execution_engine.check_and_process_completed_works(timeout=1.0)

                if completed_jobs:
                    logger.debug(f"Processed {len(completed_jobs)} completed jobs")

                pending_snapshot = self.execution_engine.get_pending_works_snapshot()
                if not pending_snapshot:
                    time.sleep(self.poll_interval)
                else:
                    time.sleep(1.0)

            except Exception as e:
                logger.error(f"❌ Error in Ray monitor thread: {str(e)}", exc_info=True)
                time.sleep(self.poll_interval)

        logger.info("⚡ Ray Work Monitor stopped")

    def start(self):
        """Daemon 시작"""
        if self.run_once:
            logger.info("Starting EventDrivenLogger (run_once mode)...")
        else:
            logger.info("Starting EventDrivenLogger (continuous mode)...")

        if self.running:
            logger.warning("Logger is already running")
            return

        self.running = True
        self.stop_event.clear()

        self.listener_thread = Thread(
            target=self._listener_thread_func,
            daemon=True,
            name="DBChangeListener"
        )
        self.listener_thread.start()

        self.monitor_thread = Thread(
            target=self._ray_monitor_thread_func,
            daemon=True,
            name="RayWorkMonitor"
        )
        self.monitor_thread.start()
        logger.info("✓ Ray Work Monitor thread started")

        # run_once 모드면 스레드 완료 대기
        if self.run_once:
            self.listener_thread.join()
            self.monitor_thread.join()
            self.running = False
            logger.info("✅ EventDrivenLogger completed (run_once mode)")
        else:
            logger.info("✅ EventDrivenLogger started successfully (continuous mode)")

    def stop(self):
        """Daemon 중지"""
        logger.info("Stopping EventDrivenDaemon...")

        self.running = False
        self.stop_event.set()

        # 스레드 종료 대기
        if self.listener_thread:
            self.listener_thread.join(timeout=5)
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)

        logger.info("✅ EventDrivenDaemon stopped")


def main(run_once: bool = False):
    """
    EventDrivenLogger 실행
    
    Args:
        run_once: True면 1회만 실행, False면 계속 실행 (기본값)
    """
    initialize_logger()
    event_logger = EventDrivenLogger(poll_interval=2.0, run_once=run_once)
    event_logger.start()

    if run_once:
        # run_once 모드: start()가 완료되면 종료
        logger.info("✅ Run once mode completed")
    else:
        # continuous 모드: 메인 스레드 유지
        logger.info("이벤트 로거가 백그라운드에서 실행 중...")

        try:
            while True:
                time.sleep(1)
                # 여기서 다른 작업 가능
        except KeyboardInterrupt:
            logger.info("종료 신호 받음...")
            event_logger.stop()


if __name__ == "__main__":
    import logging
    from sv.utils.logger import setup_common_logger

    # ray.init(num_cpus=8)
    ray.init(local_mode=True)
    run_once_mode = False

    # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    log_level = logging.DEBUG  # DEBUG로 설정하면 모든 detail 메시지 출력
    # log_level = logging.INFO   # INFO로 설정하면 info 이상의 메시지만 출력

    # 공통 로거 초기화 (로그 레벨 지정)
    setup_common_logger(level=log_level)

    if run_once_mode:
        logger.info("🔄 Run once mode enabled")
    else:
        logger.info("♾️ Continuous mode enabled")

    main(run_once=run_once_mode)
