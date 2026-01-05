"""
FLineDaemon 사용 예시

순차적으로 실행될 작업들을 등록하고 daemon을 시작합니다.
"""

import logging

from sv.daemon import FLineDaemon
from sv.tasks import (
    VideoProcessingTask,
    AnalysisTask,
    ReportGenerationTask,
    NotificationTask
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """메인 함수 - Daemon 시작"""
    
    # 1. Daemon 초기화 (15분 간격으로 루프 실행)
    daemon = FLineDaemon(
        interval_minutes=1,  # 테스트를 위해 1분으로 설정 (실제는 15분 등으로 설정)
        num_executors=3,      # 3개의 Executor 사용
        use_actors=True       # Actor 기반 실행
    )
    
    # 2. 순차적으로 실행할 작업 등록
    # 다음 순서로 실행됨:
    #   1. VideoProcessingTask
    #   2. AnalysisTask (VideoProcessing 결과 사용 가능)
    #   3. ReportGenerationTask (VideoProcessing, Analysis 결과 사용 가능)
    #   4. NotificationTask (모든 이전 작업 결과 사용 가능)
    
    tasks = [
        VideoProcessingTask(),
        AnalysisTask(),
        ReportGenerationTask(),
        NotificationTask()
    ]
    
    daemon.register_sequential_tasks(tasks)
    
    logger.info("=" * 80)
    logger.info("🚀 FLine Daemon Starting")
    logger.info("=" * 80)
    logger.info(f"✓ Interval: 1 minute")
    logger.info(f"✓ Executors: 3")
    logger.info(f"✓ Sequential tasks: {len(tasks)}")
    for task in tasks:
        logger.info(f"  - {task.task_name}")
    logger.info("=" * 80)
    
    # 3. Daemon 시작
    try:
        daemon.start()
    except Exception as e:
        logger.error(f"Daemon error: {str(e)}")
        daemon.shutdown()


def example_custom_task():
    """사용자 정의 작업을 추가하는 예시"""
    
    from sv.executor import TaskBase
    
    class CustomTask(TaskBase):
        """사용자 정의 작업 예시"""
        
        def __init__(self):
            super().__init__("CustomTask")
        
        def execute(self, context):
            """작업 실행"""
            self.logger.info("Executing custom task...")
            self.logger.info(f"Previous results: {list(context.keys())}")
            
            # 이전 작업 결과 활용
            video_result = context.get("VideoProcessing")
            if video_result:
                self.logger.info(f"Using video count: {video_result.get('videos_processed')}")
            
            # 실제 작업 로직
            import time
            time.sleep(1)
            
            return {
                "status": "success",
                "custom_data": "processed"
            }
    
    # Daemon 생성 및 작업 등록
    daemon = FLineDaemon(
        interval_minutes=5,
        num_executors=2,
        use_actors=True
    )
    
    # 커스텀 작업 등록
    daemon.register_sequential_task(CustomTask())
    daemon.register_sequential_task(VideoProcessingTask())
    
    daemon.start()


if __name__ == '__main__':
    # 기본 예시 실행
    main()
    
    # 또는 커스텀 작업 예시 실행
    # example_custom_task()

