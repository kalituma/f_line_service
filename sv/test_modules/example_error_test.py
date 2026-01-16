"""
에러 발생 테스트 예제

split_executor에서 특정 Task 실행 중 에러를 발생시키는 다양한 방법을 보여줍니다.
"""
from typing import Dict, Any, List
from sv.test_modules.test_tasks import ErrorTestTask, ConnectionTask
from sv.task.mock.video_extract_task import VideoExtractTask


# ============================================================
# 방법 1: ErrorTestTask 사용 (가장 유연함)
# ============================================================

def example_1_always_fail():
    """항상 에러를 발생시키는 Task"""
    task = ErrorTestTask(
        task_name="AlwaysFailTask",
        error_type="RuntimeError",
        error_message="이 Task는 항상 실패합니다"
    )
    return task


def example_2_fail_on_third_item():
    """3번째 아이템에서만 에러를 발생시키는 Task"""
    task = ErrorTestTask(
        task_name="FailOnThirdItem",
        error_type="ValueError",
        error_message="3번째 아이템 처리 중 에러",
        fail_on_item_index=2  # 0-based index
    )
    return task


def example_3_fail_on_condition():
    """특정 조건일 때만 에러를 발생시키는 Task"""
    def should_fail(context: Dict[str, Any]) -> bool:
        # 예: frfr_id가 특정 값일 때만 실패
        item = context.get('item', {})
        frfr_id = item.get('frfr_id', '')
        return frfr_id == '123456'  # 이 ID일 때만 실패
    
    task = ErrorTestTask(
        task_name="ConditionalFailTask",
        error_type="KeyError",
        error_message="특정 조건에서 실패",
        fail_on_condition=should_fail
    )
    return task


def example_4_multiple_error_types():
    """다양한 에러 타입 테스트"""
    tasks = []
    
    # ValueError
    tasks.append(ErrorTestTask(
        task_name="ValueErrorTask",
        error_type="ValueError",
        error_message="잘못된 값"
    ))
    
    # RuntimeError
    tasks.append(ErrorTestTask(
        task_name="RuntimeErrorTask",
        error_type="RuntimeError",
        error_message="런타임 에러"
    ))
    
    # KeyError
    tasks.append(ErrorTestTask(
        task_name="KeyErrorTask",
        error_type="KeyError",
        error_message="키를 찾을 수 없음"
    ))
    
    return tasks


# ============================================================
# 방법 2: TaskBase의 raise_exception 파라미터 사용
# ============================================================

def example_5_use_raise_exception():
    """TaskBase의 raise_exception 파라미터 사용"""
    task = VideoExtractTask(work_dir="/tmp/test")
    
    # 에러를 발생시키도록 설정
    task.raise_exception = RuntimeError("비디오 추출 중 의도적 에러")
    
    return task


# ============================================================
# 방법 3: 기존 Task 클래스 수정 (ConnectionTask)
# ============================================================

def example_6_connection_task_fail():
    """ConnectionTask에 추가된 should_fail 파라미터 사용"""
    task = ConnectionTask(
        api_url="http://example.com/api",
        should_fail=True,
        fail_message="API 연결 실패 시뮬레이션"
    )
    return task


# ============================================================
# 실제 사용 예제: split_executor와 함께
# ============================================================

def create_test_scenario_1():
    """
    시나리오 1: 두 번째 secondary task에서 에러 발생
    - VideoExtractTask: 정상 실행
    - ErrorTestTask: 에러 발생
    - ConnectionTask: 실행 안 됨 (이전 Task 실패로)
    """
    from sv.task.mock.video_extract_task import VideoExtractTask
    
    primary_task = VideoExtractTask(work_dir="/tmp/test")
    
    secondary_tasks = [
        VideoExtractTask(work_dir="/tmp/test"),  # 1번째 - 성공
        ErrorTestTask(  # 2번째 - 실패 (여기서 멈춤)
            task_name="SecondTaskFail",
            error_type="RuntimeError",
            error_message="두 번째 Task에서 실패"
        ),
        ConnectionTask(api_url="http://example.com")  # 3번째 - 실행 안됨
    ]
    
    return primary_task, secondary_tasks


def create_test_scenario_2():
    """
    시나리오 2: 특정 아이템에서만 에러 발생
    continue_on_error=True로 설정하면 다음 아이템 계속 처리
    """
    from sv.task.mock.video_extract_task import VideoExtractTask
    
    primary_task = VideoExtractTask(work_dir="/tmp/test")
    
    secondary_tasks = [
        VideoExtractTask(work_dir="/tmp/test"),
        ErrorTestTask(
            task_name="FailOnSecondItem",
            error_type="ValueError",
            error_message="2번째 아이템 처리 실패",
            fail_on_item_index=1  # 두 번째 아이템에서만 실패
        ),
        ConnectionTask(api_url="http://example.com")
    ]
    
    return primary_task, secondary_tasks


# ============================================================
# 테스트 실행 함수
# ============================================================

def run_split_executor_with_error_test():
    """
    실제 split_executor에서 에러 테스트를 실행하는 예제
    """
    from sv.daemon.module.split_executor import SplitExecutor
    from sv.utils.logger import setup_logger
    
    logger = setup_logger(__name__)
    
    # 시나리오 선택
    primary_task, secondary_tasks = create_test_scenario_1()
    # 또는
    # primary_task, secondary_tasks = create_test_scenario_2()
    
    # SplitExecutor 생성 및 실행
    executor = SplitExecutor(job_id=1)
    
    def mock_splitter(result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """테스트용 데이터 분할기"""
        return [
            {"frfr_id": "123456", "analysis_id": "A001"},
            {"frfr_id": "123457", "analysis_id": "A002"},
            {"frfr_id": "123458", "analysis_id": "A003"}
        ]
    
    try:
        result = executor.execute_with_data_splitting(
            primary_task=primary_task,
            secondary_tasks=secondary_tasks,
            loop_context={},
            data_splitter=mock_splitter,
            continue_on_error=True  # False로 하면 첫 에러에서 중단
        )
        
        logger.info("=" * 80)
        logger.info("테스트 실행 결과:")
        logger.info(f"  전체 상태: {result['status']}")
        logger.info(f"  성공 아이템: {result['success_count']}")
        logger.info(f"  실패 아이템: {result['error_count']}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"테스트 실행 중 에러: {str(e)}", exc_info=True)


if __name__ == "__main__":
    print("=" * 80)
    print("에러 발생 테스트 예제")
    print("=" * 80)
    print()
    print("이 스크립트는 split_executor에서 에러를 발생시키는 다양한 방법을 보여줍니다.")
    print()
    print("사용 가능한 방법:")
    print("  1. ErrorTestTask - 가장 유연한 방법")
    print("  2. TaskBase.raise_exception - 간단한 방법")
    print("  3. 기존 Task 수정 (ConnectionTask.should_fail)")
    print()
    print("=" * 80)
    
    # 실제 테스트 실행 (필요시 주석 해제)
    # run_split_executor_with_error_test()

