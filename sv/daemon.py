import requests
from apscheduler.schedulers.blocking import BlockingScheduler
import ray
from datetime import datetime
import logging
from typing import List, Callable, Dict, Any

from sv.monitor import FlineTaskMonitor
from sv.executor import FlineTaskExecutor, TaskBase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FLineDaemon:
    """순차적 작업 실행을 지원하는 Ray Daemon"""
    
    def __init__(
        self,
        interval_seconds: int = 900,
        num_executors: int = 5,
        use_actors: bool = True,
        sequential_tasks: List[TaskBase] = None
    ):
        """
        Args:
            interval_seconds: 루프 실행 간격 (초)
            num_executors: 실행자(Executor) 개수
            use_actors: Actor 사용 여부
            sequential_tasks: 순차적으로 실행할 작업 목록
        """
        self.interval_seconds = interval_seconds
        self.sequential_tasks = sequential_tasks or []
        self.task_monitor = FlineTaskMonitor(
            num_executors=num_executors,
            use_actors=use_actors
        )
        self.scheduler = BlockingScheduler()
        self.num_executors = num_executors
        self.use_actors = use_actors
        
        # Ray Actor 풀 생성
        if use_actors:
            self.executor_actors = [
                FlineTaskExecutor.remote(i) for i in range(num_executors)
            ]
        else:
            self.executor_actors = None
        
        self.current_executor_idx = 0
        
        logger.info(f"FLineDaemon initialized")
        logger.info(f"  Interval: {interval_seconds} seconds")
        logger.info(f"  Sequential tasks: {len(self.sequential_tasks)}")
        logger.info(f"  Executors: {num_executors}")

    def register_sequential_task(self, task: TaskBase):
        """순차 작업 등록
        
        Args:
            task: 실행할 작업 (TaskBase 인스턴스)
        """
        self.sequential_tasks.append(task)
        logger.info(f"Task registered: {task.task_name}")

    def register_sequential_tasks(self, tasks: List[TaskBase]):
        """여러 순차 작업 등록
        
        Args:
            tasks: 실행할 작업 리스트
        """
        self.sequential_tasks.extend(tasks)
        logger.info(f"{len(tasks)} tasks registered")

    def loop_trigger(self):
        """주기적으로 실행되는 루프 - 순차 작업 실행"""
        
        if not self.sequential_tasks:
            logger.warning("No sequential tasks registered")
            return
        
        logger.info("=" * 80)
        logger.info(f"🔄 Loop triggered at {datetime.now().isoformat()}")
        logger.info("=" * 80)
        
        try:
            # 루프 컨텍스트 생성
            loop_context = {
                'loop_time': datetime.now().isoformat(),
                'interval_seconds': self.interval_seconds,
                'task_count': len(self.sequential_tasks)
            }
            
            # Actor에서 순차 작업 실행
            executor_actor = self.executor_actors[self.current_executor_idx]
            self.current_executor_idx = (self.current_executor_idx + 1) % len(self.executor_actors)
            
            logger.info(f"Submitting {len(self.sequential_tasks)} sequential tasks to Executor")
            
            # 원격 함수로 순차 작업 실행
            result_ref = executor_actor.execute_sequential_tasks.remote(
                self.sequential_tasks,
                loop_context
            )
            
            # 결과 대기 및 수신
            result = ray.get(result_ref)
            
            # 결과 처리
            self._handle_loop_result(result)
            
            logger.info("=" * 80)
            logger.info(f"✅ Loop completed successfully")
            logger.info("=" * 80)
        
        except Exception as e:
            logger.error(f"❌ Error in loop_trigger: {str(e)}", exc_info=True)
            logger.info("=" * 80)

    def _handle_loop_result(self, result: Dict[str, Any]):
        """루프 실행 결과 처리"""
        logger.info(f"Loop execution result:")
        logger.info(f"  Status: {result.get('status')}")
        logger.info(f"  Total duration: {result.get('total_duration'):.2f}s")
        logger.info(f"  Tasks executed: {len(result.get('tasks', []))}")
        
        for task_info in result.get('tasks', []):
            task_name = task_info.get('task_name', 'unknown')
            task_status = task_info.get('status', 'unknown')
            task_duration = task_info.get('duration', 0)
            
            status_icon = "✓" if task_status == 'success' else "✗"
            logger.info(f"  {status_icon} {task_name}: {task_status} ({task_duration:.2f}s)")

    def fetch_and_trigger(self):
        """API 호출 및 작업 트리거 (기존 방식)"""

        # 먼저 완료된 작업 확인
        self.task_monitor.check_completed_tasks()

        try:
            response = requests.get(self.api_url, timeout=10)
            response.raise_for_status()

            tasks = response.json()
            if isinstance(tasks, dict):
                tasks = [tasks]

            logger.info(f"Fetched {len(tasks)} tasks")

            # 작업 제출
            for task in tasks:
                self.task_monitor.submit_task(task)

            # 상태 로깅
            status = self.task_monitor.get_status()
            logger.info(f"Status: {status}")

            # Ray 클러스터 상태
            logger.info(f"Ray resources: {ray.available_resources()}")

        except Exception as e:
            logger.error(f"Error in fetch_and_trigger: {str(e)}")

    def start(self):
        """데몬 시작"""
        logger.info("Starting Ray daemon...")
        logger.info(f"Ray cluster info: {ray.cluster_resources()}")

        # 지정된 간격으로 루프 실행
        logger.info(f"Scheduling loop to run every {self.interval_seconds} seconds")
        self.scheduler.add_job(
            self.loop_trigger,
            'interval',
            seconds=self.interval_seconds,
            id='loop_trigger_job',
            next_run_time=datetime.now()
        )

        # 1분마다 완료된 작업 확인 (선택적)
        # self.scheduler.add_job(
        #     self.task_monitor.check_completed_tasks,
        #     'interval',
        #     minutes=1,
        #     id='check_completion_job'
        # )

        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Received shutdown signal")
            self.shutdown()

    def shutdown(self):
        """종료"""
        logger.info("Shutting down Ray daemon...")
        self.scheduler.shutdown(wait=False)
        self.task_monitor.shutdown()
        ray.shutdown()
        logger.info("Ray daemon stopped")


if __name__ == '__main__':
    # 사용 예시
    daemon = FLineDaemon(
        interval_minutes=15,
        num_executors=5,
        use_actors=True
    )
    
    daemon.start()