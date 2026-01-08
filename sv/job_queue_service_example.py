"""
JobQueueService 사용 예제

이 파일은 JobQueueService의 다양한 사용 방법을 보여줍니다.
"""

import logging
from job_queue_service import JobQueueService
from sv.backend.db.job_queue_db import JobStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_basic_job_operations():
    """기본 작업 추가 및 조회 예제"""
    print("\n=== 기본 작업 추가 및 조회 ===")
    
    service = JobQueueService(db_path="example_jobs.db")
    
    # 1. 새 작업 추가
    job_id = service.add_job("FIRE_20230101_SEOUL", "ANALYSIS_20230101_001")
    print(f"✓ 작업 추가됨: job_id={job_id}")
    
    # 2. 작업 조회
    job = service.get_job_by_id(job_id)
    print(f"✓ 작업 정보: {job}")
    
    # 3. 다음 pending 작업 가져오기
    next_job_id = service.get_next_job()
    print(f"✓ 처리할 작업: job_id={next_job_id}")


def example_job_with_tasks():
    """작업과 태스크 함께 생성 예제"""
    print("\n=== 작업과 태스크 함께 생성 ===")
    
    service = JobQueueService(db_path="example_jobs.db")
    
    # 1. 작업과 태스크 함께 생성
    task_names = ["video_extract", "frame_analysis", "data_save", "result_send"]
    job_id = service.add_job_with_tasks(
        frfr_id="FIRE_20230102_BUSAN",
        analysis_id="ANALYSIS_20230102_001",
        task_names=task_names
    )
    print(f"✓ 작업 생성됨: job_id={job_id}")
    
    # 2. 작업의 태스크 조회
    tasks = service.get_job_tasks(job_id)
    print(f"✓ 태스크 목록 (총 {len(tasks)}개):")
    for task in tasks:
        print(f"  - {task['task_name']}: {task['status']}")


def example_status_management():
    """작업 및 태스크 상태 관리 예제"""
    print("\n=== 작업 및 태스크 상태 관리 ===")
    
    service = JobQueueService(db_path="example_jobs.db")
    
    # 1. 새 작업 추가
    job_id = service.add_job("FIRE_20230103_DAEGU", "ANALYSIS_20230103_001")
    print(f"✓ 작업 생성: job_id={job_id}")
    
    # 2. 작업 상태 업데이트
    service.update_job_status(job_id, JobStatus.PROCESSING)
    print(f"✓ 작업 상태 변경: PENDING → PROCESSING")
    
    # 3. 태스크 초기화
    service.initialize_tasks(job_id, ["extract", "analyze"])
    print(f"✓ 태스크 초기화됨")
    
    # 4. 태스크 상태 업데이트
    tasks = service.get_job_tasks(job_id)
    if tasks:
        first_task_id = tasks[0]['task_id']
        service.update_task_status(first_task_id, JobStatus.COMPLETED)
        print(f"✓ 첫 번째 태스크 상태 변경: PENDING → COMPLETED")


def example_query_by_status():
    """상태별 작업 조회 예제"""
    print("\n=== 상태별 작업 조회 ===")
    
    service = JobQueueService(db_path="example_jobs.db")
    
    # 1. Pending 작업 조회
    pending_jobs = service.get_jobs_by_status(JobStatus.PENDING)
    print(f"✓ Pending 작업 개수: {len(pending_jobs)}")
    for job in pending_jobs[:3]:  # 처음 3개만 출력
        print(f"  - job_id={job['job_id']}, frfr_id={job['frfr_id']}")
    
    # 2. Processing 작업 조회
    processing_jobs = service.get_jobs_by_status(JobStatus.PROCESSING)
    print(f"✓ Processing 작업 개수: {len(processing_jobs)}")
    
    # 3. 완료된 작업 조회
    completed_jobs = service.get_jobs_by_status(JobStatus.COMPLETED)
    print(f"✓ Completed 작업 개수: {len(completed_jobs)}")


def example_workflow():
    """전체 워크플로우 예제"""
    print("\n=== 전체 워크플로우 예제 ===")
    
    service = JobQueueService(db_path="example_jobs.db")
    
    # 1단계: 작업 추가
    print("\n[1단계] 새 작업 추가...")
    job_id = service.add_job_with_tasks(
        frfr_id="FIRE_20230104_INCHEON",
        analysis_id="ANALYSIS_20230104_001",
        task_names=["video_extract", "motion_detect", "save_result"]
    )
    print(f"✓ 작업 생성: job_id={job_id}")
    
    # 2단계: 다음 작업 가져오기 및 처리 시작
    print("\n[2단계] 다음 작업 처리...")
    next_job_id = service.get_next_job()
    if next_job_id:
        job = service.get_job_by_id(next_job_id)
        print(f"✓ 처리 중인 작업: {next_job_id}")
        print(f"  - frfr_id: {job['frfr_id']}")
        print(f"  - 상태: {job['status']}")
    
    # 3단계: 태스크 처리 시뮬레이션
    print("\n[3단계] 태스크 처리...")
    tasks = service.get_job_tasks(job_id)
    for idx, task in enumerate(tasks, 1):
        service.update_task_status(task['task_id'], JobStatus.PROCESSING)
        print(f"  - {task['task_name']}: 처리 중...")
        # 작업 처리 시뮬레이션
        service.update_task_status(task['task_id'], JobStatus.COMPLETED)
        print(f"  - {task['task_name']}: 완료 ✓")
    
    # 4단계: 작업 완료
    print("\n[4단계] 작업 완료...")
    service.update_job_status(job_id, JobStatus.COMPLETED)
    print(f"✓ 작업 완료: job_id={job_id}")
    
    # 5단계: 최종 상태 조회
    print("\n[5단계] 최종 상태 조회...")
    final_job = service.get_job_by_id(job_id)
    final_tasks = service.get_job_tasks(job_id)
    print(f"✓ 작업 상태: {final_job['status']}")
    print(f"✓ 완료된 태스크: {sum(1 for t in final_tasks if t['status'] == 'completed')}/{len(final_tasks)}")


def example_all_jobs():
    """모든 작업 조회 예제"""
    print("\n=== 모든 작업 조회 ===")
    
    service = JobQueueService(db_path="example_jobs.db")
    
    all_jobs = service.get_all_jobs()
    print(f"✓ 전체 작업 개수: {len(all_jobs)}")
    print("\n작업 목록:")
    for job in all_jobs[:5]:  # 처음 5개만 출력
        print(f"  - job_id={job['job_id']}, frfr_id={job['frfr_id']}, status={job['status']}")


if __name__ == "__main__":
    print("=" * 60)
    print("JobQueueService 사용 예제")
    print("=" * 60)
    
    # 각 예제 실행
    example_basic_job_operations()
    example_job_with_tasks()
    example_status_management()
    example_query_by_status()
    example_workflow()
    example_all_jobs()
    
    print("\n" + "=" * 60)
    print("모든 예제 완료!")
    print("=" * 60)

