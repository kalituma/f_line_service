"""
Task 실행 예제 (Debug용)

Ray 없이 task를 순차적으로 실행하면서 context를 명시적으로 전달합니다.
IDE의 디버거에서 각 task의 실행을 단계적으로 추적할 수 있습니다.
"""

import logging
from typing import Dict, Any

from sv.task.tasks import (
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


def execute_tasks_sequentially(tasks: list) -> Dict[str, Any]:
    """
    Task들을 순차적으로 실행하면서 context를 전달합니다.
    
    Args:
        tasks: 실행할 task 인스턴스들의 리스트
        
    Returns:
        모든 task 결과를 담은 context 딕셔너리
    """
    context = {}
    
    logger.info("=" * 80)
    logger.info("🚀 Task Execution Started (Sequential, No Ray)")
    logger.info("=" * 80)
    logger.info(f"📋 Total tasks to execute: {len(tasks)}")
    for idx, task in enumerate(tasks, 1):
        logger.info(f"  {idx}. {task.task_name}")
    logger.info("=" * 80)
    
    for idx, task in enumerate(tasks, 1):
        try:
            logger.info("")
            logger.info(f"[{idx}/{len(tasks)}] 🔄 Executing task: {task.task_name}")
            logger.info(f"     Context keys: {list(context.keys())}")
            
            # Task 실행 - context를 명시적으로 전달
            task_result = task.execute(context)
            
            # Context에 결과 추가 (task_name을 key로 사용)
            context[task.task_name] = task_result
            
            logger.info("     ✅ Task completed successfully")
            logger.info(f"     Result: {task_result}")
            logger.info("")
        
        except Exception as e:
            logger.error(f"     ❌ Task failed: {str(e)}", exc_info=True)
            logger.error(f"     Stopping task execution at: {task.task_name}")
            raise
    
    logger.info("=" * 80)
    logger.info("✅ All tasks executed successfully")
    logger.info("=" * 80)
    logger.info(f"📊 Final context keys: {list(context.keys())}")
    
    return context


def main():
    """메인 함수 - Task 1회 실행"""
    
    logger.info("=" * 80)
    logger.info("📌 Debug Mode: Sequential Task Execution")
    logger.info("=" * 80)
    
    # Task 인스턴스 생성
    tasks = [
        VideoProcessingTask(),
        AnalysisTask(),
        ReportGenerationTask(),
        NotificationTask()
    ]
    
    try:
        # 순차적으로 task 실행
        final_context = execute_tasks_sequentially(tasks)
        
        # 최종 결과 출력
        logger.info("")
        logger.info("=" * 80)
        logger.info("📈 Final Results Summary:")
        logger.info("=" * 80)
        
        for task_name, result in final_context.items():
            logger.info(f"\n[{task_name}]")
            logger.info(f"  Status: {result.get('status')}")
            for key, value in result.items():
                if key != 'status':
                    logger.info(f"  {key}: {value}")
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("🎉 Task execution completed successfully!")
        logger.info("=" * 80)
        
        return final_context
    
    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ Task execution failed!")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        raise


def debug_single_task():
    """
    개별 task 디버깅 예제
    특정 task만 실행하고 싶을 때 사용합니다.
    """
    logger.info("=" * 80)
    logger.info("🔍 Debug Single Task")
    logger.info("=" * 80)
    
    # 분석 task만 실행
    analysis_task = AnalysisTask()
    
    # 이전 작업의 결과를 시뮬레이션
    mock_context = {
        "VideoProcessing": {
            "status": "success",
            "videos_processed": 5,
            "output_path": "/data/output/videos"
        }
    }
    
    logger.info("Executing AnalysisTask with mock context...")
    logger.info(f"Mock context: {mock_context}")
    
    try:
        result = analysis_task.execute(mock_context)
        logger.info(f"✅ Task result: {result}")
    except Exception as e:
        logger.error(f"❌ Task failed: {str(e)}", exc_info=True)


def debug_with_custom_context():
    """
    Custom context를 전달하여 특정 시나리오를 테스트하는 예제
    """
    logger.info("=" * 80)
    logger.info("🧪 Debug with Custom Context")
    logger.info("=" * 80)
    
    # 모든 task 인스턴스 생성
    tasks = [
        VideoProcessingTask(),
        AnalysisTask(),
        ReportGenerationTask(),
        NotificationTask()
    ]
    
    # Custom context로 시작
    custom_context = {
        "custom_flag": "debug_mode",
        "test_id": "TEST_001"
    }
    
    logger.info(f"Starting with custom context: {custom_context}")
    
    # Task 실행
    for task in tasks:
        try:
            logger.info(f"\n🔄 Executing: {task.task_name}")
            result = task.execute(custom_context)
            custom_context[task.task_name] = result
            logger.info(f"✅ Completed: {task.task_name}")
        except Exception as e:
            logger.error(f"❌ Failed at {task.task_name}: {str(e)}")
            break
    
    return custom_context


if __name__ == '__main__':
    # 기본 실행 (모든 task 순차 실행)
    main()