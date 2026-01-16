from typing import Dict, Any, Optional
from datetime import datetime

try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

from sv import LOG_DIR_PATH
from sv.utils.logger import setup_common_logger, setup_logger

from sv.backend.service.app_state_manager import get_app_state_manager, AppState
from sv.backend.service.service_manager import get_service_manager

from sv.daemon.module.recovery_check import RecoveryCheckService
from sv.daemon.module.work_manager import WorkManager
from sv.daemon.module.task_manager import TaskManager
from sv.daemon.module.event_processor import EventProcessor
from sv.daemon.module.execution_engine import ExecutionEngine
from sv.daemon.module.thread_manager import ThreadManager
from sv.daemon.module.update_handler import send_video_status_update
from sv.daemon.module.http_request_client import post_request, HttpRequestError

from sv.daemon.server_state import ServerAnalysisStatus
from sv.daemon.daemon_state import STATUS_FAILED, STATUS_SUCCESS, STATUS_NOT_EXISTS

from sv.routers.fline_web_server import init_web_server

logger = setup_logger(__name__)

class FlineDaemon:
    """
    이벤트 기반 Daemon
    """

    def __init__(
        self,
        base_work_dir: str,
        analysis_update_url,
        result_sent_url,
        num_executors: int = 2,
        poll_interval: float = 2.0,
        web_host: str = "localhost",
        web_port: int = 8090
    ):
        """
        Args:
            num_executors: Ray Actor 개수
            poll_interval: Event Listener 폴링 간격
        """
        self.analysis_update_url = analysis_update_url
        self.result_sent_url = result_sent_url

        self.service_manager = get_service_manager()
        self.recovery_service = RecoveryCheckService()
        self.setup_recovery_tasks()

        self.web_app = init_web_server(self)
        self.app_state = get_app_state_manager()

        # ==================== 컴포넌트 초기화 ====================
        self.work_manager = WorkManager()
        self.task_manager = TaskManager()

        self.event_processor = EventProcessor(
            poll_interval=poll_interval,
            on_work_created=self._on_work_created
        ) # including listener

        # ExecutionEngine 먼저 초기화
        self.execution_engine = ExecutionEngine(base_work_dir, update_url=self.analysis_update_url, num_executors=num_executors)
        
        # ThreadManager 초기화 시 ExecutionEngine 전달
        self.thread_manager = ThreadManager(
            poll_interval=poll_interval,
            web_app=self.web_app,
            web_host=web_host,
            web_port=web_port,
            execution_engine=self.execution_engine  # Ray Job Monitor를 위해 전달
        )
        
        # Thread Manager 콜백 설정
        self.thread_manager.set_check_changes_callback(
            self.event_processor.check_changes
        )
        
        logger.info("=" * 80)
        logger.info("   FlineDaemon initialized")
        logger.info(f"  Executors: {num_executors}")
        logger.info(f"  Poll Interval: {poll_interval}s")
        logger.info("=" * 80)
    
    # ==================== Task Management API ====================
    
    def register_primary_task(self, task):
        """Primary Task 등록"""
        self.task_manager.register_primary_task(task)
    
    def register_secondary_tasks(self, tasks):
        """Secondary Tasks 등록"""
        self.task_manager.register_secondary_tasks(tasks)
    
    def set_data_splitter(self, splitter):
        """데이터 분할 함수 등록"""
        self.task_manager.set_data_splitter(splitter)

    def setup_recovery_tasks(self):
        """RecoveryCheckService에 초기화 작업 등록"""

        # Task 1: Processing to Pending 준비
        async def change_works_to_pending():
            """
            시스템 복구 시 PROCESSING 상태의 모든 작업을 PENDING으로 변경
            (비정상 종료 후 재시작 시 진행 중이던 작업 복구)
            """
            logger.info("=" * 80)
            logger.info("🔄 Recovering works from PROCESSING to PENDING...")
            logger.info("=" * 80)

            try:
                from sv.backend.work_status import WorkStatus

                # WorkQueueService 가져오기
                work_queue_service = self.service_manager.get_work_queue_service()
                if not work_queue_service:
                    logger.error("❌ WorkQueueService not available")
                    return False

                # PROCESSING 상태의 모든 작업 조회
                logger.info("📋 Fetching works with PROCESSING status...")
                processing_works = work_queue_service.get_works_by_status(WorkStatus.PROCESSING)

                if not processing_works:
                    logger.info("✓ No works in PROCESSING status")
                    return True

                logger.info(f"📊 Found {len(processing_works)} works in PROCESSING status")

                # PROCESSING 상태의 작업을 PENDING으로 변경
                success_count = 0
                failed_count = 0

                for work in processing_works:
                    work_id = work.get('work_id')
                    logger.info(f"  → Updating work_id={work_id} to PENDING...")

                    try:
                        result = work_queue_service.update_work_status(work_id, WorkStatus.PENDING)
                        if result:
                            success_count += 1
                            logger.info(f"    ✓ work_id={work_id} changed to PENDING")
                        else:
                            failed_count += 1
                            logger.warning(f"    ✗ Failed to change work_id={work_id} status")
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"    ✗ Error updating work_id={work_id}: {str(e)}")

                logger.info("=" * 80)
                logger.info(f"✓ Recovery complete: {success_count} succeeded, {failed_count} failed")
                logger.info("=" * 80)

                return failed_count == 0

            except Exception as e:
                logger.error("=" * 80)
                logger.error(f"❌ Error during work recovery: {str(e)}")
                logger.error("=" * 80)
                return False

        # 초기화 작업 등록
        self.recovery_service.add_task("change_works_to_pending", change_works_to_pending)

    # ==================== Internal Event Handlers ====================
    
    def _on_work_created(self) -> None:
        """Job 생성 감지 시 핸들러 (Event Processor에서 호출)"""
        logger.info("Job creation event detected")
        self._process_pending_work()
    
    def _process_pending_work(self) -> None:
        """
        대기 중인 Job 처리 (비동기 콜백 방식)
        
        Flow:
        1. 다음 PENDING Job 가져오기
        2. Task 실행 (논블로킹)
        3. 완료 시 _on_job_complete 콜백 호출
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

    async def restore_and_init(self) -> bool:
        """
        복구 및 초기화 수행 (RecoveryCheckService 실행)

        Returns:
            bool: 성공 여부
        """
        logger.info("=" * 80)
        logger.info("Starting Daemon Initialization...")
        logger.info("=" * 80)

        logger.info("=" * 80)
        logger.info("Initializing all services...")
        logger.info("=" * 80)

        service_init_success = self.service_manager.initialize_all_services()

        if not service_init_success:
            await self.app_state.set_state(AppState.SHUTDOWN)
            logger.error("=" * 80)
            logger.error("❌ Service Initialization FAILED!")
            logger.error("=" * 80)
            return False

        # RecoveryCheckService의 모든 작업 실행
        success = await self.recovery_service.run_all()

        if success:
            await self.app_state.set_state(AppState.READY)
            logger.info("=" * 80)
            logger.info("✅ Daemon is READY!")
            logger.info("=" * 80)
        else:
            await self.app_state.set_state(AppState.SHUTDOWN)
            logger.error("=" * 80)
            logger.error("❌ Daemon Initialization FAILED!")
            logger.error("=" * 80)

        return success

    # ==================== Lifecycle Management ====================
    
    def start(self) -> None:
        """Daemon 시작"""
        logger.info("Starting FlineDaemon...")
        self.thread_manager.start()
        logger.info("FlineDaemon started successfully")
    
    def stop(self) -> None:
        """Daemon 중지"""
        logger.info("Stopping FlineDaemon...")
        self.thread_manager.stop()
        logger.info("FlineDaemon stopped")