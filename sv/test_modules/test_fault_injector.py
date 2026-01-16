"""
FaultInjector 사용 예제

FlineTaskSplitExecutor에서 선택적으로 exception을 발생시키는 방법을 보여줍니다.
"""

import ray
from sv.daemon.module.fault_injector import (
    FaultInjector,
    FaultInjectorBuilder,
    create_simple_injector,
    create_chaos_injector
)
from sv.daemon.module.split_executor import FlineTaskSplitExecutor


def example_1_simple_fault_injection():
    """예제 1: 간단한 Fault Injection"""
    print("\n" + "=" * 80)
    print("예제 1: 간단한 Fault Injection")
    print("=" * 80)
    
    # 1. FaultInjector 생성 및 fault 등록
    injector = FaultInjector()
    
    # _create_work_directory에서 항상 에러 발생
    injector.register_fault(
        method_name="_create_work_directory",
        exception=RuntimeError("디렉토리 생성 실패 시뮬레이션"),
        probability=1.0  # 100% 발생
    )
    
    # 2. Executor에 주입
    executor = FlineTaskSplitExecutor.remote(
        executor_id=1,
        base_work_dir="/path/to/work",
        update_url="http://example.com",
        fault_injector=injector
    )
    
    print("✓ FaultInjector가 주입된 Executor 생성 완료")
    print("  → _create_work_directory에서 RuntimeError가 발생합니다")


def example_2_probability_based():
    """예제 2: 확률 기반 Fault Injection"""
    print("\n" + "=" * 80)
    print("예제 2: 확률 기반 Fault Injection")
    print("=" * 80)
    
    # 1. FaultInjector 생성
    injector = FaultInjector()
    
    # _execute_primary_task에서 30% 확률로 에러 발생
    injector.register_fault(
        method_name="_execute_primary_task",
        exception=TimeoutError("타임아웃 시뮬레이션"),
        probability=0.3  # 30% 확률
    )
    
    # _split_data에서 50% 확률로 에러 발생
    injector.register_fault(
        method_name="_split_data",
        exception=ValueError("데이터 분할 실패"),
        probability=0.5  # 50% 확률
    )
    
    # 2. Executor에 주입
    executor = FlineTaskSplitExecutor.remote(
        executor_id=2,
        base_work_dir="/path/to/work",
        update_url="http://example.com",
        fault_injector=injector
    )
    
    print("✓ 확률 기반 FaultInjector가 주입된 Executor 생성 완료")
    print("  → _execute_primary_task: 30% 확률로 TimeoutError")
    print("  → _split_data: 50% 확률로 ValueError")


def example_3_conditional_fault():
    """예제 3: 조건부 Fault Injection"""
    print("\n" + "=" * 80)
    print("예제 3: 조건부 Fault Injection")
    print("=" * 80)
    
    # 1. FaultInjector 생성
    injector = FaultInjector()
    
    # 데이터 개수가 5개 이상일 때만 에러 발생
    injector.register_fault(
        method_name="_process_data_items",
        exception=MemoryError("데이터가 너무 많음"),
        condition=lambda ctx: ctx.get('data_item_count', 0) >= 5
    )
    
    # work_id가 특정 값일 때만 에러 발생
    injector.register_fault(
        method_name="_create_work_directory",
        exception=PermissionError("권한 없음"),
        condition=lambda ctx: ctx.get('work_id') == 12345
    )
    
    # 2. Executor에 주입
    executor = FlineTaskSplitExecutor.remote(
        executor_id=3,
        base_work_dir="/path/to/work",
        update_url="http://example.com",
        fault_injector=injector
    )
    
    print("✓ 조건부 FaultInjector가 주입된 Executor 생성 완료")
    print("  → _process_data_items: 데이터 5개 이상일 때 MemoryError")
    print("  → _create_work_directory: work_id가 12345일 때 PermissionError")


def example_4_max_triggers():
    """예제 4: 최대 트리거 횟수 제한"""
    print("\n" + "=" * 80)
    print("예제 4: 최대 트리거 횟수 제한")
    print("=" * 80)
    
    # 1. FaultInjector 생성
    injector = FaultInjector()
    
    # 처음 3번만 에러 발생 (그 이후는 정상 동작)
    injector.register_fault(
        method_name="_execute_secondary_tasks",
        exception=ConnectionError("네트워크 불안정"),
        max_triggers=3  # 최대 3번만 발생
    )
    
    # 2. Executor에 주입
    executor = FlineTaskSplitExecutor.remote(
        executor_id=4,
        base_work_dir="/path/to/work",
        update_url="http://example.com",
        fault_injector=injector
    )
    
    print("✓ 최대 트리거 제한이 있는 FaultInjector가 주입된 Executor 생성 완료")
    print("  → _execute_secondary_tasks: 처음 3번만 ConnectionError 발생")


def example_5_builder_pattern():
    """예제 5: Builder 패턴 사용"""
    print("\n" + "=" * 80)
    print("예제 5: Builder 패턴 사용")
    print("=" * 80)
    
    # Fluent API로 FaultInjector 생성
    injector = (
        FaultInjectorBuilder()
        .with_fault("_create_work_directory", RuntimeError("디렉토리 생성 실패"))
        .with_probability(0.5)
        .with_fault("_split_data", ValueError("분할 실패"))
        .with_probability(0.3)
        .with_max_triggers(5)
        .with_fault("_execute_primary_task", TimeoutError("타임아웃"))
        .with_condition(lambda ctx: ctx.get('retry_count', 0) > 3)
        .build()
    )
    
    # Executor에 주입
    executor = FlineTaskSplitExecutor.remote(
        executor_id=5,
        base_work_dir="/path/to/work",
        update_url="http://example.com",
        fault_injector=injector
    )
    
    print("✓ Builder 패턴으로 생성된 FaultInjector가 주입된 Executor 생성 완료")
    print("  → _create_work_directory: 50% 확률로 RuntimeError")
    print("  → _split_data: 30% 확률로 ValueError (최대 5번)")
    print("  → _execute_primary_task: retry_count > 3일 때 TimeoutError")


def example_6_simple_helper():
    """예제 6: 간편 헬퍼 함수 사용"""
    print("\n" + "=" * 80)
    print("예제 6: 간편 헬퍼 함수 사용")
    print("=" * 80)
    
    # 1. create_simple_injector 사용
    injector = create_simple_injector({
        "_create_work_directory": RuntimeError("디렉토리 생성 실패"),
        "_split_data": ValueError("데이터 분할 실패"),
        "_execute_primary_task": TimeoutError("타임아웃")
    })
    
    # 2. Executor에 주입
    executor = FlineTaskSplitExecutor.remote(
        executor_id=6,
        base_work_dir="/path/to/work",
        update_url="http://example.com",
        fault_injector=injector
    )
    
    print("✓ 간편 헬퍼로 생성된 FaultInjector가 주입된 Executor 생성 완료")
    print("  → 3개 메서드에서 각각 100% 확률로 에러 발생")


def example_7_chaos_engineering():
    """예제 7: Chaos Engineering 모드"""
    print("\n" + "=" * 80)
    print("예제 7: Chaos Engineering 모드")
    print("=" * 80)
    
    # 모든 메서드에서 랜덤하게 에러 발생 (10% 확률)
    injector = create_chaos_injector(
        methods=[
            "_create_work_directory",
            "_execute_primary_task",
            "_split_data",
            "_create_video_directories",
            "_process_data_items",
            "_execute_secondary_tasks",
            "_on_secondary_tasks_start"
        ],
        exceptions=[
            RuntimeError("랜덤 에러"),
            TimeoutError("타임아웃"),
            ConnectionError("네트워크 에러"),
            MemoryError("메모리 부족")
        ],
        probability=0.1  # 각 메서드에서 10% 확률
    )
    
    # Executor에 주입
    executor = FlineTaskSplitExecutor.remote(
        executor_id=7,
        base_work_dir="/path/to/work",
        update_url="http://example.com",
        fault_injector=injector
    )
    
    print("✓ Chaos Engineering 모드의 FaultInjector가 주입된 Executor 생성 완료")
    print("  → 모든 메서드에서 10% 확률로 랜덤 에러 발생")


def example_8_no_fault_injection():
    """예제 8: Fault Injection 없이 사용 (기존 동작)"""
    print("\n" + "=" * 80)
    print("예제 8: Fault Injection 없이 사용")
    print("=" * 80)
    
    # fault_injector를 전달하지 않음 (기본값 None)
    executor = FlineTaskSplitExecutor.remote(
        executor_id=8,
        base_work_dir="/path/to/work",
        update_url="http://example.com"
        # fault_injector 파라미터 없음
    )
    
    print("✓ Fault Injection 없는 일반 Executor 생성 완료")
    print("  → 기존과 동일하게 정상 동작합니다")


def example_9_disable_enable():
    """예제 9: Fault Injection 활성화/비활성화"""
    print("\n" + "=" * 80)
    print("예제 9: Fault Injection 활성화/비활성화")
    print("=" * 80)
    
    # 1. FaultInjector 생성 (비활성화 상태로)
    injector = FaultInjector(enabled=False)
    
    # Fault 등록
    injector.register_fault(
        method_name="_create_work_directory",
        exception=RuntimeError("디렉토리 생성 실패")
    )
    
    print("✓ 비활성화 상태의 FaultInjector 생성")
    print("  → Fault가 등록되어 있지만 발생하지 않습니다")
    
    # 2. 활성화
    injector.enabled = True
    print("✓ FaultInjector 활성화")
    print("  → 이제 Fault가 발생합니다")
    
    # 3. 다시 비활성화
    injector.enabled = False
    print("✓ FaultInjector 비활성화")
    print("  → 다시 Fault가 발생하지 않습니다")


def example_10_statistics():
    """예제 10: Fault Injection 통계 확인"""
    print("\n" + "=" * 80)
    print("예제 10: Fault Injection 통계 확인")
    print("=" * 80)
    
    # FaultInjector 생성
    injector = FaultInjector()
    
    # 여러 fault 등록
    injector.register_fault(
        method_name="_create_work_directory",
        exception=RuntimeError("에러 1"),
        max_triggers=5
    )
    injector.register_fault(
        method_name="_split_data",
        exception=ValueError("에러 2"),
        probability=0.3,
        condition=lambda ctx: True
    )
    
    # 통계 확인
    stats = injector.get_statistics()
    
    print("✓ Fault Injection 통계:")
    for method_name, stat in stats.items():
        print(f"\n  메서드: {method_name}")
        print(f"    - 트리거 횟수: {stat['trigger_count']}")
        print(f"    - 최대 트리거: {stat['max_triggers']}")
        print(f"    - 발생 확률: {stat['probability'] * 100}%")
        print(f"    - 조건 함수: {'있음' if stat['has_condition'] else '없음'}")


def main():
    """모든 예제 실행"""
    print("\n" + "=" * 80)
    print("FaultInjector 사용 예제 모음")
    print("=" * 80)
    
    # Ray 초기화 (필요한 경우)
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    
    # 모든 예제 실행
    example_1_simple_fault_injection()
    example_2_probability_based()
    example_3_conditional_fault()
    example_4_max_triggers()
    example_5_builder_pattern()
    example_6_simple_helper()
    example_7_chaos_engineering()
    example_8_no_fault_injection()
    example_9_disable_enable()
    example_10_statistics()
    
    print("\n" + "=" * 80)
    print("모든 예제 완료!")
    print("=" * 80)


if __name__ == "__main__":
    main()

