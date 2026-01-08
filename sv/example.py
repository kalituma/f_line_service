"""
FlineDaemon 사용 예제 (리팩토링 후)

Main Flow:
1. Daemon 생성
2. Task 등록
3. Daemon 시작
4. Main Loop에서 job_add() 호출
5. 자동으로 처리됨
"""

import ray
from sv.daemon import FlineDaemon
from sv.task.task_base import TaskBase
from typing import Dict, Any


# ==================== Task 정의 ====================

class DataCollectionTask(TaskBase):
    """Task 1: 데이터 수집"""
    
    def __init__(self):
        super().__init__("data_collection")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        self.logger.info("Collecting data...")
        
        # 예시: 데이터 반환
        return {
            'items': [
                {'id': 1, 'name': 'item1'},
                {'id': 2, 'name': 'item2'},
                {'id': 3, 'name': 'item3'},
            ]
        }


class ProcessingTask(TaskBase):
    """Task 2: 각 아이템 처리"""
    
    def __init__(self):
        super().__init__("processing")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        item = context.get('current_data')
        self.logger.info(f"Processing {item}...")
        
        return {'processed': True, 'item_id': item['id']}


class SavingTask(TaskBase):
    """Task 3: 결과 저장"""
    
    def __init__(self):
        super().__init__("saving")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        item = context.get('current_data')
        self.logger.info(f"Saving {item}...")
        
        return {'saved': True, 'item_id': item['id']}


# ==================== 데이터 분할 함수 ====================

def split_items(result: Dict[str, Any]):
    """Task 1 결과를 개별 아이템으로 분할"""
    items = result.get('items', [])
    print(f"📦 Splitting {len(items)} items")
    return items


# ==================== Main ====================

def main():
    """메인 함수"""
    
    # 1. Ray 초기화
    ray.init(num_cpus=4, ignore_reinit_error=True)
    print("✓ Ray initialized")
    
    # 2. Daemon 생성
    daemon = FlineDaemon(
        num_executors=2,
        poll_interval=2.0,
        enable_event_listener=True,
        enable_fallback_polling=True
    )
    print("✓ Daemon created")
    
    # 3. Task 등록
    daemon.register_primary_task(DataCollectionTask())
    daemon.register_secondary_tasks([
        ProcessingTask(),
        SavingTask()
    ])
    daemon.set_data_splitter(split_items)
    print("✓ Tasks registered")
    
    # 4. Daemon 시작
    daemon.start()
    print("✓ Daemon started")
    
    # 5. Job 추가 (Main Loop)
    print("\n--- Adding jobs ---")
    job_id_1 = daemon.add_job("fire_001", "analysis_001")
    job_id_2 = daemon.add_job("fire_002", "analysis_002")
    
    # 6. 상태 모니터링
    import time
    for i in range(10):
        time.sleep(1)
        print(f"[{i}s] {daemon.get_summary()}")
    
    # 7. Job 상태 확인
    if job_id_1:
        status = daemon.get_job_status(job_id_1)
        print(f"Job {job_id_1} status: {status}")
    
    # 8. Daemon 종료
    daemon.stop()
    print("✓ Daemon stopped")


if __name__ == '__main__':
    main()

