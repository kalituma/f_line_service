import requests
from apscheduler.schedulers.blocking import BlockingScheduler
import ray
from datetime import datetime
import logging

from sv.monitor import FlineTaskMonitor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FLineDaemon:
    def __init__(self, api_url: str, num_executors=5, use_actors=True):
        self.api_url = api_url
        self.task_monitor = FlineTaskMonitor(
            num_executors=num_executors,
            use_actors=use_actors
        )
        self.scheduler = BlockingScheduler()

    def fetch_and_trigger(self):
        """API 호출 및 작업 트리거"""
        logger.info(f"Fetching tasks from {self.api_url}")

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

        # 15분마다 실행
        self.scheduler.add_job(
            self.fetch_and_trigger,
            'interval',
            minutes=15,
            id='ray_trigger_job',
            next_run_time=datetime.now()
        )

        # 1분마다 완료된 작업 확인 (선택적)
        self.scheduler.add_job(
            self.task_monitor.check_completed_tasks,
            'interval',
            minutes=1,
            id='check_completion_job'
        )

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
        api_url='https://your-api.com/tasks',
        num_executors=5,
        use_actors=True  # False면 stateless 함수 사용
    )
    
    daemon.start()