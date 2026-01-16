try:
    import ray  # type: ignore
except ImportError:
    ray = None  # type: ignore

import os
from datetime import datetime
from typing import Dict, Any, List, Callable, Optional

from sv import DEFAULT_JOB_QUEUE_DB
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger
from sv.daemon.server_state import ServerAnalysisStatus
from sv.daemon.module.http_request_client import HttpRequestError
from sv.daemon.module.update_handler import send_video_status_update
from sv.daemon.daemon_state import STATUS_SUCCESS, STATUS_FAILED, STATUS_NOT_EXISTS
from sv.daemon.module.fault_injector import FaultInjector


@ray.remote
class FlineTaskSplitExecutor:
    """Ray Actor로 순차 작업을 실행하는 클래스 (데이터 분할 지원)"""

    def __init__(self, executor_id: int, base_work_dir: str, update_url: str, 
                 fault_injector: Optional[FaultInjector] = None):
        self.executor_id = executor_id
        self.base_work_dir = base_work_dir
        self.update_url = update_url
        self.logger = setup_logger(f"Executor-{executor_id}")
        self._job_queue_service = None  # Lazy initialization
        self.fault_injector = fault_injector  # Optional fault injection

    @property
    def job_queue_service(self):
        """필요할 때만 서비스 생성 (각 Ray actor 프로세스에서 독립적으로)"""
        if self._job_queue_service is None:
            from sv.backend.service.job_queue_service import JobQueueService

            self._job_queue_service = JobQueueService(DEFAULT_JOB_QUEUE_DB)
            self.logger.info(f"✓ JobQueueService initialized in Ray actor with db_path={DEFAULT_JOB_QUEUE_DB}")

        return self._job_queue_service
    
    def _inject_fault(self, method_name: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Fault injection 체크 (fault_injector가 설정된 경우에만)
        
        Args:
            method_name: 현재 메서드 이름
            context: 메서드 실행 컨텍스트
        """
        if self.fault_injector:
            self.fault_injector.inject(method_name, context)

    def execute_sequential_tasks(
            self,
            tasks: List[TaskBase],
            loop_context: Dict[str, Any],
            data_processor: Optional[Callable[[Dict[str, Any]], Any]] = None
    ) -> Dict[str, Any]:
        """
        순차적으로 여러 작업을 실행합니다.
        
        Args:
            tasks: 실행할 작업 목록 (TaskBase 인스턴스)
            loop_context: 루프의 컨텍스트 정보
            data_processor: 첫 번째 Task 결과를 처리할 함수 (데이터 분할 등)
            
        Returns:
            전체 실행 결과
            
        Example:
            ```python
            def split_data(result):
                # Task 1의 결과가 리스트라면 개별 아이템 반환
                return result.get('items', [])
            
            result = executor.execute_sequential_tasks(
                tasks=[task1, task2, task3],
                loop_context={'job_id': 123},
                data_processor=split_data
            )
            ```
        """
        self.logger.info(f"Executor {self.executor_id} executing {len(tasks)} sequential tasks")
        self.logger.info(f"Loop context: {loop_context}")

        results = {
            'executor_id': self.executor_id,
            'loop_context': loop_context,
            'tasks': [],
            'status': STATUS_SUCCESS.to_str(),
            'completed_at': datetime.now().isoformat(),
            'total_duration': 0,
            'error_count': 0,
            'error': None  # 최상단 에러 메시지
        }

        start_time = datetime.now()
        task_context = {}  # 작업들 간 컨텍스트 공유

        for idx, task in enumerate(tasks, 1):
            try:
                self.logger.info(f"[{idx}/{len(tasks)}] Executing task: {task.task_name}")

                task_start = datetime.now()
                task_result = task.execute(task_context)
                task_duration = (datetime.now() - task_start).total_seconds()

                # 작업 결과를 컨텍스트에 추가
                task_context[task.task_name] = task_result

                self.logger.info(
                    f"[{idx}/{len(tasks)}] Task '{task.task_name}' completed "
                    f"({task_duration:.2f}s)"
                )

                results['tasks'].append({
                    'task_name': task.task_name,
                    'status': STATUS_SUCCESS.to_str(),
                    'result': task_result,
                    'duration': task_duration
                })

            except Exception as e:
                error_msg = f"Task '{task.task_name}' failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)

                # 표준 에러 설정 (최상단에 첫 번째 에러만 기록)
                if not results.get('error'):
                    results['error'] = error_msg
                results['status'] = STATUS_FAILED.to_str()
                results['error_count'] += 1
                
                results['tasks'].append({
                    'task_name': task.task_name,
                    'status': STATUS_FAILED.to_str(),
                    'error': str(e)
                })

                # 에러 발생 시 다음 작업은 계속 실행할지 선택
                # 여기서는 계속 실행하도록 설정 (필요시 break로 중단)
                continue

        total_duration = (datetime.now() - start_time).total_seconds()
        results['total_duration'] = total_duration
        results['task_context'] = task_context

        self.logger.info(
            f"Sequential task execution completed "
            f"(Status: {results['status']}, Total: {total_duration:.2f}s, Errors: {results['error_count']})"
        )

        return results

    def _create_work_directory(self, loop_context: Dict[str, Any]) -> tuple:
        """
        Job 작업 디렉토리 생성
        
        Returns:
            (job_work_dir, error_result) - 성공시 (path, None), 실패시 (None, error_dict)
        """
        # Fault injection 체크
        self._inject_fault("_create_work_directory", loop_context)
        
        work_id = loop_context.get('work_id')
        frfr_id = loop_context.get('frfr_id')
        analysis_id = loop_context.get('analysis_id')

        work_folder_name = f"{work_id}_{frfr_id}_{analysis_id}"
        job_work_dir = os.path.join(self.base_work_dir, work_folder_name)

        try:
            os.makedirs(job_work_dir, exist_ok=True)
            self.logger.info(f"✓ Job work directory created: {job_work_dir}")
            return job_work_dir, None
        except Exception as e:
            error_msg = f"Failed to create job work directory: {str(e)}"
            self.logger.error(f"❌ {error_msg}", exc_info=True)
            error_result = self._create_error_result(loop_context, error_msg)
            return None, error_result

    def _initialize_results(self, loop_context: Dict[str, Any], job_work_dir: str) -> Dict[str, Any]:
        """결과 딕셔너리 초기화"""
        return {
            'executor_id': self.executor_id,
            'loop_context': loop_context,
            'job_work_dir': job_work_dir,
            'primary_task': None,
            'item_results': [],
            'status': STATUS_SUCCESS.to_str(),
            'completed_at': datetime.now().isoformat(),
            'total_duration': 0,
            'error_count': 0,
            'error': None  # 최상단 에러 메시지 (on_job_complete에서 사용)
        }
    
    def _create_error_result(self, loop_context: Dict[str, Any], error_msg: str, 
                            job_work_dir: str = None, duration: float = 0) -> Dict[str, Any]:
        """
        에러 결과를 표준 형식으로 생성
        
        Args:
            loop_context: 루프 컨텍스트
            error_msg: 에러 메시지
            job_work_dir: 작업 디렉토리 (없으면 None)
            duration: 실행 시간
            
        Returns:
            표준화된 에러 결과 딕셔너리
        """
        return {
            'executor_id': self.executor_id,
            'loop_context': loop_context,
            'job_work_dir': job_work_dir,
            'status': STATUS_FAILED.to_str(),
            'error': error_msg,  # 최상단에 에러 메시지 (on_job_complete가 읽음)
            'completed_at': datetime.now().isoformat(),
            'total_duration': duration,
            'error_count': 1
        }
    
    def _set_error_on_results(self, results: Dict[str, Any], error_msg: str) -> None:
        """
        기존 results에 에러 정보를 설정 (상태 변경)
        
        Args:
            results: 결과 딕셔너리
            error_msg: 에러 메시지
        """
        results['status'] = STATUS_FAILED.to_str()
        results['error'] = error_msg  # 최상단에 에러 메시지 설정
        results['error_count'] += 1

    def _execute_primary_task(self, primary_task: TaskBase, task_context: Dict[str, Any],
                              results: Dict[str, Any]) -> tuple:
        """
        Primary task 실행
        
        Returns:
            (primary_result, is_success)
        """
        # Fault injection 체크

        try:
            self.logger.info(f"[1] Executing primary task: {primary_task.task_name}")
            self._inject_fault("_execute_primary_task", task_context)

            primary_start = datetime.now()
            primary_result = primary_task.execute(task_context)
            primary_duration = (datetime.now() - primary_start).total_seconds()

            task_context[primary_task.task_name] = primary_result

            self.logger.info(
                f"[1] Primary task '{primary_task.task_name}' completed ({primary_duration:.2f}s)"
            )

            results['primary_task'] = {
                'task_name': primary_task.task_name,
                'status': STATUS_SUCCESS.to_str(),
                'result': primary_result,
                'duration': primary_duration
            }

            return primary_result, True

        except Exception as e:
            error_msg = f"Primary task '{primary_task.task_name}' failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            # 표준 에러 설정 (최상단에 error 추가)
            self._set_error_on_results(results, error_msg)
            results['primary_task'] = {
                'task_name': primary_task.task_name,
                'status': STATUS_FAILED.to_str(),
                'error': str(e)
            }

            return None, False

    def _split_data(self, primary_result: Any, data_splitter: Callable,
                    results: Dict[str, Any]) -> tuple:
        """
        데이터 분할
        
        Returns:
            (data_items, is_success)
        """
        # Fault injection 체크

        try:
            self.logger.info("[2] Splitting data from primary task result")
            self._inject_fault("_split_data", {'primary_result': primary_result})

            data_items = data_splitter(primary_result)

            if not isinstance(data_items, list):
                data_items = [data_items]

            self.logger.info(f"[2] Data split into {len(data_items)} items")

            return data_items, True

        except Exception as e:
            error_msg = f"Data splitting failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            # 표준 에러 설정
            self._set_error_on_results(results, error_msg)

            return None, False

    def _execute_secondary_tasks(self, secondary_tasks: List[TaskBase], item_context: Dict[str, Any],
                                 data_idx: int, total_items: int,
                                 continue_on_error: bool) -> tuple:
        """
        Secondary tasks 실행
        
        Returns:
            (item_result, should_break)
        """
        # Fault injection 체크
        # self._inject_fault("_execute_secondary_tasks", {
        #     'data_idx': data_idx,
        #     'total_items': total_items,
        #     **item_context
        # })
        
        item_result = {
            'tasks': [],
            'status': STATUS_SUCCESS.to_str()
        }
        should_break = False

        for task_seq, task in enumerate(secondary_tasks, 1):
            try:
                self.logger.info(
                    f"[3.{data_idx}/{total_items}.{task_seq}/{len(secondary_tasks)}] "
                    f"Executing task: {task.task_name}"
                )

                task_start = datetime.now()
                task_result = task.execute(item_context)
                task_duration = (datetime.now() - task_start).total_seconds()

                item_context[task.task_name] = task_result

                self.logger.info(
                    f"[3.{data_idx}/{total_items}.{task_seq}/{len(secondary_tasks)}] "
                    f"Task '{task.task_name}' completed ({task_duration:.2f}s)"
                )

                item_result['tasks'].append({
                    'task_name': task.task_name,
                    'status': STATUS_SUCCESS.to_str(),
                    'result': task_result,
                    'duration': task_duration
                })

            except Exception as e:
                error_msg = f"Task '{task.task_name}' failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)

                item_result['status'] = STATUS_FAILED.to_str()
                item_result['tasks'].append({
                    'task_name': task.task_name,
                    'status': STATUS_FAILED.to_str(),
                    'error': str(e)
                })

                if not continue_on_error:
                    should_break = True
                    break

        return item_result, should_break

    def _update_job_status(self, job_id: Optional[int], status_str: str, data_idx: int, total_items: int) -> None:
        """
        Job 상태를 업데이트합니다.
        
        Args:
            job_id: 업데이트할 job의 ID
            status_str: 변경할 상태 ('success', 'failed', 'not_exists' 등)
            data_idx: 현재 데이터 인덱스 (로깅용)
            total_items: 전체 데이터 개수 (로깅용)
        """
        if not job_id:
            self.logger.warning(
                f"[3.{data_idx}/{total_items}.update] ⊘ Job ID is None (스킵)"
            )
            return
        
        try:
            from sv.backend.work_status import WorkStatus
            
            # status_str에 따라 WorkStatus 매핑
            status_map = {
                'success': WorkStatus.COMPLETED,
                'failed': WorkStatus.FAILED,
                'not_exists': WorkStatus.FAILED,  # 파일 없음도 실패로 처리
            }
            
            work_status = status_map.get(status_str, WorkStatus.FAILED)
            status_display = work_status.value.upper()

            self.logger.info(
                f"[3.{data_idx}/{total_items}.update] "
                f"Updating job status to {status_display}: job_id={job_id}"
            )

            self.job_queue_service.update_job_status(
                job_id=job_id,
                status=work_status
            )

            self.logger.info(
                f"[3.{data_idx}/{total_items}.update] "
                f"✓ Job status updated to {status_display}: job_id={job_id}"
            )
        except Exception as e:
            self.logger.error(
                f"[3.{data_idx}/{total_items}.update] "
                f"❌ Failed to update job status: job_id={job_id}, status={status_str}, error={str(e)}",
                exc_info=True
            )

    def _finalize_item_result(self, item_result: Dict[str, Any], item_start: datetime, 
                              results: Dict[str, Any], data_idx: int, total_items: int) -> None:
        """
        아이템 처리 결과를 완료하고 Job 상태를 업데이트합니다.
        
        - duration 계산 및 설정
        - results에 item_result 추가
        - item_result['status']에 따라 job status 업데이트
        
        Args:
            item_result: 아이템 처리 결과 딕셔너리
            item_start: 아이템 처리 시작 시간
            results: 전체 결과 딕셔너리
            data_idx: 현재 데이터 인덱스
            total_items: 전체 데이터 개수
        """
        # Duration 계산 및 설정
        item_result['duration'] = (datetime.now() - item_start).total_seconds()
        
        # 결과에 추가
        results['item_results'].append(item_result)
        
        # Job 상태 업데이트 (item_result['status']에 따라)
        if item_result['status'] == STATUS_SUCCESS.to_str():
            status_str = 'success'
        elif item_result['status'] == STATUS_NOT_EXISTS.to_str():
            status_str = 'not_exists'
        else:  # STATUS_FAILED
            status_str = 'failed'
        
        self._update_job_status(
            job_id=item_result.get('job_id'),
            status_str=status_str,
            data_idx=data_idx,
            total_items=total_items
        )

    def _process_data_items(self, data_items: List[Any], video_dirs: List[Any], secondary_tasks: List[TaskBase],
                            task_context: Dict[str, Any], loop_context: Dict[str, Any],
                            continue_on_error: bool, results: Dict[str, Any]) -> None:
        """각 데이터 아이템별 처리"""
        
        for data_idx, (data_item, vid_dir) in enumerate(zip(data_items, video_dirs), 1):
            self.logger.info(f"[3.{data_idx}/{len(data_items)}] Processing data item: {data_item}")

            item_context = task_context.copy()
            item_context['job_dir'] = vid_dir
            item_context['current_data'] = data_item
            item_context['data_index'] = data_idx - 1
            item_context['total_data_items'] = len(data_items)

            item_result = {
                'data_item': data_item,
                'data_index': data_idx - 1,
                'status': STATUS_SUCCESS.to_str()
            }

            item_start = datetime.now()

            # ==================== Step 3.1: Pre-secondary 콜백 실행 ====================
            try:
                self.logger.info(
                    f"[3.{data_idx}/{len(data_items)}.pre] Executing pre-secondary callback"
                )
                item_result['job_id'] = self._on_secondary_tasks_start(data_item, vid_dir, loop_context)

                self.logger.info(
                    f"[3.{data_idx}/{len(data_items)}.pre] Pre-secondary callback completed successfully"
                )
            except FileNotFoundError as e:
                item_result['status'] = STATUS_NOT_EXISTS.to_str()
                item_result['error'] = f"File not found: {str(e)}"
                results['error_count'] += 1
                
                # 결과 완료 및 Job 상태 업데이트
                self._finalize_item_result(item_result, item_start, results, data_idx, len(data_items))
                
                if not continue_on_error:
                    break
                continue
            except Exception as e:
                error_msg = f"Pre-secondary callback failed for data_item {data_idx}: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                item_result['status'] = STATUS_FAILED.to_str()
                item_result['error'] = error_msg
                results['error_count'] += 1
                
                # 결과 완료 및 Job 상태 업데이트
                self._finalize_item_result(item_result, item_start, results, data_idx, len(data_items))
                
                if not continue_on_error:
                    break
                continue

            # ==================== Step 3.2: Secondary Tasks 실행 ====================
            secondary_result, should_break = self._execute_secondary_tasks(
                secondary_tasks, item_context, data_idx, len(data_items), continue_on_error
            )

            item_result['tasks'] = secondary_result['tasks']
            if secondary_result['status'] == 'failed':
                item_result['status'] = 'failed'
                results['error_count'] += 1
                
                if should_break:
                    # 결과 완료 및 Job 상태 업데이트 (break 전)
                    self._finalize_item_result(item_result, item_start, results, data_idx, len(data_items))
                    break

            # ==================== Step 3.3: 결과 완료 및 Job Status 업데이트 ====================
            self._finalize_item_result(item_result, item_start, results, data_idx, len(data_items))

    def execute_with_data_splitting(
            self,
            primary_task: TaskBase,
            secondary_tasks: List[TaskBase],
            loop_context: Dict[str, Any],
            data_splitter: Callable[[Any], List[Any]],
            continue_on_error: bool = True,
    ) -> Dict[str, Any]:
        """
        Task 1을 실행 후 결과를 분할하여 각 데이터에 대해 순차 Task들을 실행
        
        **실행 흐름:**
        1. Job 작업 디렉토리 생성
        2. Primary task 실행
        3. 데이터 분할
        4. 각 데이터 아이템별 처리:
           - Pre-secondary 콜백 실행
           - Secondary tasks 실행
        
        Args:
            primary_task: 먼저 실행할 주요 작업 (Task 1)
            secondary_tasks: 분할된 각 데이터에 대해 실행할 작업 리스트
            loop_context: 루프의 컨텍스트 정보
            data_splitter: 주요 작업 결과를 리스트로 분할하는 함수
            continue_on_error: 에러 발생 시 계속 실행할지 여부
            
        Returns:
            전체 실행 결과
        """

        start_time = datetime.now()

        # ==================== Step 0: 작업 루트 디렉토리 생성 ====================
        work_root_dir, error_result = self._create_work_directory(loop_context)
        if error_result:
            return error_result

        results = self._initialize_results(loop_context, work_root_dir)
        task_context = {
            'work_root_dir': work_root_dir,
            'loop_context': loop_context
        }

        # ==================== Step 1: Primary task 실행 ====================
        primary_result, is_success = self._execute_primary_task(primary_task, task_context, results)
        if not is_success:
            results['total_duration'] = (datetime.now() - start_time).total_seconds()
            return results

        # ==================== Step 2: 데이터 분할 ====================
        data_items, is_success = self._split_data(primary_result, data_splitter, results)
        if not is_success:
            results['total_duration'] = (datetime.now() - start_time).total_seconds()
            return results

        # ==================== Step 2.5: Video별 폴더 생성 ====================
        video_dirs, is_success = self._create_video_directories(data_items, loop_context, work_root_dir, results)
        if not is_success:
            results['total_duration'] = (datetime.now() - start_time).total_seconds()
            return results

        # ==================== Step 3: 각 데이터 아이템별 처리 ====================
        self._process_data_items(data_items, video_dirs, secondary_tasks, task_context, loop_context, continue_on_error,
                                 results)

        # ==================== 완료 ====================
        results['total_duration'] = (datetime.now() - start_time).total_seconds()
        
        # 에러가 발생한 경우 최상단에 종합 에러 메시지 설정
        if results['error_count'] > 0 and not results.get('error'):
            results['error'] = f"{results['error_count']} item(s) failed during processing"

        self.logger.info(
            f"Data splitting task execution completed "
            f"(Total: {results['total_duration']:.2f}s, Errors: {results['error_count']})"
        )

        return results

    def _create_video_directories(self, data_items: List[Any], loop_context: Dict[str, Any],
                                  work_root_dir: str, results: Dict[str, Any]) -> tuple:
        """
        data_items의 각 video_name과 loop_context의 start_time을 '_'로 결합하여
        work_root_dir 하위에 video별 폴더를 생성합니다.
        
        Args:
            data_items: 비디오 데이터 아이템 리스트
            loop_context: 루프의 컨텍스트 정보 (start_time 포함)
            work_root_dir: 작업 루트 디렉토리 경로
            results: 결과 딕셔너리 (에러 발생 시 업데이트)
            
        Returns:
            (video_directories, is_success) - 성공시 (list, True), 실패시 (None, False)
            
        Example:
            - data_items = ['video1.mp4', 'video2.mp4']
            - loop_context = {'start_time': '2025-10-09T19:52:37'}
            - work_root_dir = '/path/to/work'
            - 생성 폴더: /path/to/work/video1.mp4_2025-10-09T19:52:37
                       /path/to/work/video2.mp4_2025-10-09T19:52:37
        """
        try:
            self.logger.info("[2.5] Creating video directories")
            
            # Fault injection 체크
            self._inject_fault("_create_video_directories", {
                'data_item_count': len(data_items),
                'work_root_dir': work_root_dir,
                **loop_context
            })

            video_directories = []
            start_time = loop_context.get('start_time')

            self.logger.info(f"     work_root_dir: {work_root_dir}")
            self.logger.info(f"     start_time: {start_time}, total items: {len(data_items)}")

            for idx, data_item in enumerate(data_items, 1):
                # data_item에서 video_name 추출
                video_name = data_item.get('video_name', 'UNKNOWN')

                # video_name과 start_time을 '_'로 결합하여 폴더명 생성
                video_dir_name = f"{start_time}_{video_name}"
                video_directory = os.path.join(work_root_dir, video_dir_name)
                video_directories.append(video_directory)

                try:
                    os.makedirs(video_directory, exist_ok=True)
                    self.logger.info(f"     [{idx}/{len(data_items)}] ✓ Video directory created: {video_directory}")
                except Exception as e:
                    error_msg = f"Failed to create video directory '{video_directory}': {str(e)}"
                    self.logger.error(f"     [{idx}/{len(data_items)}] ❌ {error_msg}", exc_info=True)
                    
                    # 디렉토리 생성 실패는 치명적이므로 즉시 중단
                    self._set_error_on_results(results, error_msg)
                    return None, False

            self.logger.info("[2.5] Video directory creation completed successfully")
            return video_directories, True

        except Exception as e:
            error_msg = f"Video directories creation failed: {str(e)}"
            self.logger.error(f"❌ {error_msg}", exc_info=True)
            
            # 표준 에러 설정
            self._set_error_on_results(results, error_msg)
            return None, False

    def _extract_callback_context(self, data_item: Dict[str, Any], loop_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        콜백 실행에 필요한 컨텍스트 데이터를 추출합니다.
        
        Args:
            data_item: 현재 처리 중인 데이터 아이템
            loop_context: Job의 루프 컨텍스트
            
        Returns:
            추출된 컨텍스트 정보 (work_id, frfr_id, analysis_id, video_name, video_url)
        """
        context = {
            'work_id': loop_context.get('work_id'),
            'frfr_id': loop_context.get('frfr_id'),
            'analysis_id': loop_context.get('analysis_id'),
            'video_name': data_item if isinstance(data_item, str) else data_item.get('video_name', 'unknown'),
            'video_url': data_item.get('video_url') if isinstance(data_item, dict) else ''
        }

        self.logger.info("=" * 80)
        self.logger.info("📤 Pre-secondary callback 실행")
        self.logger.info(f"   work_id: {context['work_id']}")
        self.logger.info(f"   frfr_id: {context['frfr_id']}, analysis_id: {context['analysis_id']}")
        self.logger.info(f"   video_name: {context['video_name']}")
        self.logger.info(f"   video_url: {context['video_url']}")
        self.logger.info(f"   update_url: {self.update_url}")
        self.logger.info("=" * 80)

        return context

    def _validate_video_file(self, video_url: str) -> tuple[bool, ServerAnalysisStatus]:
        """
        비디오 파일의 존재 여부를 확인하고 적절한 상태를 반환합니다.
        
        Args:
            video_url: 비디오 파일 경로
            
        Returns:
            (파일 존재 여부, 분석 상태)
        """
        file_exists = os.path.isfile(video_url)

        if file_exists:
            self.logger.info(f"✓ 파일 존재 확인: {video_url}")
            status = ServerAnalysisStatus.STAT_002  # 분석 준비 중
        else:
            self.logger.warning(f"⚠️  파일이 존재하지 않음: {video_url}")
            status = ServerAnalysisStatus.STAT_005  # 파일 없음 에러

        return file_exists, status

    def _register_job_to_queue(self, work_id: int, frfr_id: str, analysis_id: str,
                               video_url: str, workspace: str, file_exists: bool) -> Optional[int]:
        """
        Job Queue에 작업을 등록합니다.
        
        Args:
            work_id: Work Queue ID
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            video_url: 비디오 파일 경로
            workspace: 작업 디렉토리 경로
            file_exists: 파일 존재 여부
            
        Returns:
            등록된 job_id 또는 None (work_id가 없거나 등록 실패 시)
            
        Note:
            파일이 없어도 Job을 등록하여 추적 가능하게 합니다.
            파일이 없으면 초기 상태를 FAILED로 설정합니다.
        """
        if not work_id:
            self.logger.warning("⚠️  work_id가 없어 Job Queue 등록 건너뜀")
            return None

        if not file_exists:
            self.logger.warning("⚠️  파일이 없어 Job Queue 등록 건너뜀")
            return None

        try:
            from sv.backend.work_status import WorkStatus

            job_id = self.job_queue_service.add_job(
                work_id=work_id,
                frfr_id=frfr_id,
                analysis_id=analysis_id,
                video_url=video_url,
                workspace=workspace,
                status=WorkStatus.PENDING
            )

            if job_id:
                self.logger.info(f"✓ Job Queue에 등록 완료: job_id={job_id}, workspace={workspace}")
            else:
                self.logger.warning(f"⚠️  Job Queue 등록 실패 (중복 가능): work_id={work_id}")

            return job_id

        except Exception as e:
            self.logger.error(f"❌ Job Queue 등록 중 에러: {str(e)}", exc_info=True)
            # Job Queue 등록 실패는 치명적이지 않으므로 None 반환
            return None

    def _send_video_status(self, frfr_id: str, analysis_id: str,
                           video_name: str, analysis_status: ServerAnalysisStatus) -> None:
        """
        외부 서버에 비디오 상태를 전송합니다.
        
        Args:
            frfr_id: 산불 정보 ID
            analysis_id: 분석 ID
            video_name: 비디오 이름
            analysis_status: 분석 상태
            
        Raises:
            HttpRequestError: 상태 전송 실패 시
        """
        send_video_status_update(
            update_url=self.update_url,
            frfr_id=frfr_id,
            analysis_id=analysis_id,
            video_updates=[
                {
                    "video_name": video_name,
                    "analysis_status": analysis_status.to_code()
                }
            ]
        )
        self.logger.info(f"✓ Video Status 업데이트 완료: status={analysis_status.to_code()}")

    def _on_secondary_tasks_start(self, data_item: Dict[str, Any], vid_dir: str, loop_context: Dict[str, Any]) -> int:
        """
        각 데이터 아이템에 대해 secondary tasks 실행 직전 호출되는 콜백
        
        오케스트레이션 흐름:
        1. 컨텍스트 추출
        2. 파일 검증
        3. Job Queue 등록
        4. Video Status 전송
        5. 에러 처리

        Args:
            data_item: 현재 처리 중인 데이터 아이템 (video_name, video_url 포함)
            vid_dir: 비디오 디렉토리 경로
            loop_context: Job의 루프 컨텍스트 (work_id, frfr_id, analysis_id 등)
            
        Raises:
            FileNotFoundError: 비디오 파일이 존재하지 않을 때
            HttpRequestError: 상태 업데이트 실패 시
        """
        try:

            # 1. 컨텍스트 추출
            context = self._extract_callback_context(data_item, loop_context)

            # 2. 파일 검증 및 상태 결정
            file_exists, analysis_status = self._validate_video_file(context['video_url'])                     

            # 3. Job Queue에 등록
            job_id = self._register_job_to_queue(
                work_id=context['work_id'],
                frfr_id=context['frfr_id'],
                analysis_id=context['analysis_id'],
                video_url=context['video_url'],
                workspace=vid_dir,
                file_exists=file_exists
            )

            # 4. Video Status 전송
            self._send_video_status(
                frfr_id=context['frfr_id'],
                analysis_id=context['analysis_id'],
                video_name=context['video_name'],
                analysis_status=analysis_status
            )            

            # 5. 파일이 없으면 예외 발생
            if not file_exists:
                raise FileNotFoundError(f"비디오 파일을 찾을 수 없음: {context['video_url']}")

            return job_id

        except HttpRequestError as e:
            self.logger.error(f"❌ 데이터 업데이트 요청 실패: {str(e)}", exc_info=True)
            raise
        except FileNotFoundError:
            raise
        except Exception as e:
            self.logger.error(f"❌ 콜백 실행 중 예상치 못한 에러: {str(e)}", exc_info=True)
            raise
