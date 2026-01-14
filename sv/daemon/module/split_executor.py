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

@ray.remote
class FlineTaskSplitExecutor:
    """Ray Actor로 순차 작업을 실행하는 클래스 (데이터 분할 지원)"""

    def __init__(self, executor_id: int, base_work_dir: str, update_url: str):
        self.executor_id = executor_id
        self.base_work_dir = base_work_dir
        self.update_url = update_url
        self.logger = setup_logger(f"Executor-{executor_id}")
        self._job_queue_service = None  # Lazy initialization

    @property
    def job_queue_service(self):
        """필요할 때만 서비스 생성 (각 Ray actor 프로세스에서 독립적으로)"""
        if self._job_queue_service is None:
            from sv.backend.service.job_queue_service import JobQueueService

            self._job_queue_service = JobQueueService(DEFAULT_JOB_QUEUE_DB)
            self.logger.info(f"✓ JobQueueService initialized in Ray actor with db_path={DEFAULT_JOB_QUEUE_DB}")

        return self._job_queue_service

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
            'status': 'success',
            'completed_at': datetime.now().isoformat(),
            'total_duration': 0
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
                
                results['status'] = STATUS_FAILED.to_str()
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
            f"(Status: {results['status']}, Total: {total_duration:.2f}s)"
        )
        
        return results

    def _create_work_directory(self, loop_context: Dict[str, Any]) -> tuple:
        """
        Job 작업 디렉토리 생성
        
        Returns:
            (job_work_dir, error_result) - 성공시 (path, None), 실패시 (None, error_dict)
        """
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
            self.logger.error(f"❌ Failed to create job work directory: {str(e)}", exc_info=True)
            error_result = {
                'executor_id': self.executor_id,
                'loop_context': loop_context,
                'status': STATUS_FAILED.to_str(),
                'error': f"Failed to create job work directory: {str(e)}",
                'completed_at': datetime.now().isoformat(),
                'total_duration': 0,
                'error_count': 1
            }
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
            'error_count': 0
        }
    
    def _execute_primary_task(self, primary_task: TaskBase, task_context: Dict[str, Any], 
                             results: Dict[str, Any]) -> tuple:
        """
        Primary task 실행
        
        Returns:
            (primary_result, is_success)
        """
        try:
            self.logger.info(f"[1] Executing primary task: {primary_task.task_name}")
            
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
            
            results['status'] = STATUS_FAILED.to_str()
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
        try:
            self.logger.info("[2] Splitting data from primary task result")
            
            data_items = data_splitter(primary_result)
            
            if not isinstance(data_items, list):
                data_items = [data_items]
            
            self.logger.info(f"[2] Data split into {len(data_items)} items")
            
            return data_items, True
        
        except Exception as e:
            error_msg = f"Data splitting failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            results['status'] = STATUS_FAILED.to_str()
            results['error'] = error_msg
            
            return None, False

    def _execute_secondary_tasks(self, secondary_tasks: List[TaskBase], item_context: Dict[str, Any],
                                data_idx: int, total_items: int, 
                                continue_on_error: bool, task_name_to_id: Dict[str, int]) -> tuple:
        """
        Secondary tasks 실행
        
        Returns:
            (item_result, should_break)
        """
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
                task_result = task.execute(item_context, task_name_to_id[task.task_name])
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
    
    def _process_data_items(self, data_items: List[Any], secondary_tasks: List[TaskBase],
                           task_context: Dict[str, Any], loop_context: Dict[str, Any],
                           continue_on_error: bool, results: Dict[str, Any], task_name_to_id: Dict[str, int]) -> None:
        """각 데이터 아이템별 처리"""
        for data_idx, data_item in enumerate(data_items, 1):
            self.logger.info(f"[3.{data_idx}/{len(data_items)}] Processing data item: {data_item}")
            
            item_context = task_context.copy()
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
                self._on_secondary_tasks_start(data_item, loop_context)

                self.logger.info(
                    f"[3.{data_idx}/{len(data_items)}.pre] Pre-secondary callback completed successfully"
                )
            except FileNotFoundError as e:
                item_result['status'] = STATUS_NOT_EXISTS.to_str()
                item_result['error'] = f"File not found: {str(e)}"
                item_result['duration'] = (datetime.now() - item_start).total_seconds()
                results['item_results'].append(item_result)
                results['error_count'] += 1
                if not continue_on_error:
                    break
                continue
            except Exception as e:
                error_msg = f"Pre-secondary callback failed for data_item {data_idx}: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                item_result['status'] = STATUS_FAILED.to_str()
                item_result['error'] = error_msg
                item_result['duration'] = (datetime.now() - item_start).total_seconds()
                results['item_results'].append(item_result)
                results['error_count'] += 1
                if not continue_on_error:
                    break
                continue
            
            # ==================== Step 3.2: Secondary Tasks 실행 ====================
            secondary_result, should_break = self._execute_secondary_tasks(
                secondary_tasks, item_context, data_idx, len(data_items), continue_on_error, task_name_to_id
            )
            
            item_result['tasks'] = secondary_result['tasks']
            if secondary_result['status'] == 'failed':
                item_result['status'] = 'failed'
                results['error_count'] += 1
                if should_break:
                    item_result['duration'] = (datetime.now() - item_start).total_seconds()
                    results['item_results'].append(item_result)
                    break
            
            item_result['duration'] = (datetime.now() - item_start).total_seconds()
            results['item_results'].append(item_result)

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
            'job_work_dir': work_root_dir,
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
        video_dirs = self._create_video_directories(data_items, loop_context, work_root_dir)
        task_name_to_id = self._on_ray_execution_start(loop_context, primary_task, secondary_tasks, job_work_dir)
        
        # ==================== Step 3: 각 데이터 아이템별 처리 ====================
        self._process_data_items(data_items, secondary_tasks, task_context, loop_context, continue_on_error, results, task_name_to_id)
        
        # ==================== 완료 ====================
        results['total_duration'] = (datetime.now() - start_time).total_seconds()
        
        self.logger.info(
            f"Data splitting task execution completed "
            f"(Total: {results['total_duration']:.2f}s, Errors: {results['error_count']})"
        )
        
        return results

    def _on_ray_execution_start(self, loop_context, primary_task, secondary_tasks, job_work_dir) -> Dict[str, int]:
        # ==================== Step 0-1: Task 정보를 데이터베이스에 저장 ====================
        try:
            job_id = loop_context.get('job_id')
            tasks = [primary_task] + secondary_tasks

            # 각 task에 job_queue_service 설정
            for task in tasks:
                task.work_queue_service = self.job_queue_service

            # TaskService를 통해 task 정보 저장 (task_id 리스트 반환)
            # task_ids = None
            task_ids = self.job_queue_service.insert_tasks_from_task_objects(
                job_id=job_id,
                tasks=tasks,
                job_work_dir=job_work_dir
            )
            if task_ids:
                self.logger.debug(f"✓ Tasks saved to database: job_id={job_id}, task_ids={task_ids}")
            else:
                self.logger.error(f"❌ Failed to save tasks to database: job_id={job_id}")

            return {
                task.task_name: task_id for task, task_id in zip(tasks, task_ids)
            }
        except Exception as e:
            self.logger.error(f"❌ Error saving tasks to database: {str(e)}", exc_info=True)
            return {}

    def _create_video_directories(self, data_items: List[Any], loop_context: Dict[str, Any], 
                                 work_root_dir: str) -> List[str]:
        """
        data_items의 각 video_name과 loop_context의 start_time을 '_'로 결합하여
        work_root_dir 하위에 video별 폴더를 생성합니다.
        
        Args:
            data_items: 비디오 데이터 아이템 리스트
            loop_context: 루프의 컨텍스트 정보 (start_time 포함)
            work_root_dir: 작업 루트 디렉토리 경로
            
        Example:
            - data_items = ['video1.mp4', 'video2.mp4']
            - loop_context = {'start_time': '2025-10-09T19:52:37'}
            - work_root_dir = '/path/to/work'
            - 생성 폴더: /path/to/work/video1.mp4_2025-10-09T19:52:37
                       /path/to/work/video2.mp4_2025-10-09T19:52:37
        """
        try:
            video_directories = []
            start_time = loop_context.get('start_time')
            
            self.logger.info(f"[2.5] Creating video directories in {work_root_dir}")
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
                    self.logger.error(f"     [{idx}/{len(data_items)}] ❌ Failed to create video directory: {video_directory}")
                    self.logger.error(f"     Error: {str(e)}", exc_info=True)                    
            
            self.logger.info(f"[2.5] Video directory creation completed successfully")

            return video_directories
        
        except Exception as e:
            self.logger.error(f"❌ Failed to create video directories: {str(e)}", exc_info=True)
            raise

    def _on_secondary_tasks_start(self, data_item: Dict[str, Any], loop_context: Dict[str, Any]) -> None:
        """
        각 데이터 아이템에 대해 secondary tasks 실행 직전 호출되는 콜백

        Args:
            data_item: 현재 처리 중인 데이터 아이템 (video_name, video_url 포함)
            loop_context: Job의 루프 컨텍스트 (job_id, frfr_id, analysis_id 등)
        """
        try:
            frfr_id = loop_context.get('frfr_id')
            analysis_id = loop_context.get('analysis_id')
            job_id = loop_context.get('job_id')

            # data_item에서 video_name과 video_url 추출
            video_name = data_item if isinstance(data_item, str) else data_item.get('video_name', 'unknown')
            video_url = data_item.get('video_url') if isinstance(data_item, dict) else ''

            self.logger.info("=" * 80)
            self.logger.info(f"📤 Pre-secondary callback 실행: Job {job_id}")
            self.logger.info(f"   URL: {self.update_url}")
            self.logger.info(f"   frfr_id: {frfr_id}, analysis_id: {analysis_id}")
            self.logger.info(f"   video_name: {video_name}")
            self.logger.info(f"   video_url: {video_url}")
            self.logger.info("=" * 80)

            # 파일 존재 여부 확인
            analysis_status = ServerAnalysisStatus.STAT_002
            if not os.path.isfile(video_url):
                self.logger.warning(f"⚠️  파일이 존재하지 않음: {video_url}")
                analysis_status = ServerAnalysisStatus.STAT_005
                file_exists = False
            else:
                file_exists = True
                self.logger.info(f"✓ 파일 존재 확인: {video_url}")

            # 파일이 존재하면 STAT_002로 업데이트
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

            if not file_exists:
                raise FileNotFoundError(f"비디오 파일을 찾을 수 없음: {video_url}")

        except HttpRequestError as e:
            self.logger.error(f"❌ 데이터 업데이트 요청 실패: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"❌ 콜백 실행 중 예상치 못한 에러: {str(e)}", exc_info=True)
            raise

