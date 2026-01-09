"""
Event-Driven Daemon 빠른 시작 가이드
3단계로 바로 시작할 수 있습니다!
"""

# ============================================================
# STEP 1: Task 정의 (your_tasks.py)
# ============================================================

from sv.task.task_base import TaskBase
from typing import Dict, Any
import time


class Step1_CollectData(TaskBase):
    """Step 1: 데이터 수집 (Task 1)"""
    
    def __init__(self):
        super().__init__("collect_data")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 수집"""
        self.logger.info("Collecting data...")
        
        # 실제로는 DB에서 조회
        items = [
            {'id': 1, 'name': 'Item 1', 'value': 100},
            {'id': 2, 'name': 'Item 2', 'value': 200},
            {'id': 3, 'name': 'Item 3', 'value': 300},
        ]
        
        return {'items': items}


class Step2_ProcessItem(TaskBase):
    """Step 2: 각 아이템 처리 (Task 2)"""
    
    def __init__(self):
        super().__init__("process_item")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """개별 아이템 처리"""
        item = context.get('current_data')
        item_id = item.get('id')
        
        self.logger.info(f"Processing item {item_id}")
        time.sleep(1)  # 처리 시뮬레이션
        
        return {
            'processed': True,
            'item_id': item_id,
            'result': item['value'] * 2
        }


class Step3_SaveResult(TaskBase):
    """Step 3: 결과 저장 (Task 3)"""
    
    def __init__(self):
        super().__init__("save_result")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """결과 저장"""
        item = context.get('current_data')
        process_result = context.get('process_item', {})
        
        item_id = item.get('id')
        
        self.logger.info(f"Saving result for item {item_id}")
        time.sleep(0.5)  # 저장 시뮬레이션
        
        return {
            'saved': True,
            'item_id': item_id,
            'saved_value': process_result.get('result')
        }


# ============================================================
# STEP 2: 데이터 분할 함수 정의
# ============================================================

def split_items(result: Dict[str, Any]):
    """
    Task 1의 결과에서 아이템 리스트를 반환
    (각 아이템마다 Task 2, 3을 순차 실행)
    """
    items = result.get('items', [])
    print(f"📦 Splitting into {len(items)} items for processing")
    return items


# ============================================================
# STEP 3: Daemon 생성 및 실행
# ============================================================

def main_event_driven():
    """Event-Driven Daemon 실행"""
    
    import ray
    from sv.backup.event_driven_daemon import EventDrivenDaemon, EventDrivenDaemonConfig
    import logging
    
    # Ray 초기화
    ray.init(num_cpus=4, ignore_reinit_error=True)
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("🚀 Event-Driven Daemon 시작")
    logger.info("=" * 80)
    
    # 1️⃣ Daemon 설정
    config = EventDrivenDaemonConfig(
        num_executors=3,
        db_path="data/sqlite/jobs.db",
        poll_interval=2.0,
        fallback_poll_interval=30,
        enable_event_listener=True,
        enable_fallback_polling=True
    )
    
    # 2️⃣ Daemon 생성
    daemon = EventDrivenDaemon(config)
    
    # 3️⃣ Task 등록
    daemon.register_primary_task(Step1_CollectData())
    daemon.register_secondary_tasks([
        Step2_ProcessItem(),
        Step3_SaveResult()
    ])
    daemon.set_data_splitter(split_items)
    
    # 4️⃣ Daemon 시작
    daemon.start()
    
    logger.info("✅ Daemon 시작됨")
    logger.info(f"Status: {daemon.get_status()}")
    
    # 5️⃣ Job 생성 (테스트)
    logger.info("\n📝 Job 생성 중...")
    job_id = daemon.job_queue.add_job("fire_001", "analysis_001")
    
    if job_id:
        logger.info(f"✅ Job 생성됨: job_id={job_id}")
        
        # Job이 처리될 때까지 대기
        import time as time_module
        time_module.sleep(15)  # 15초 대기
        
        logger.info("\n📊 최종 결과 (로그 확인)")
    
    # 6️⃣ Daemon 종료
    logger.info("\n🛑 Daemon 종료 중...")
    daemon.stop()
    ray.shutdown()
    
    logger.info("✅ 완료!")


# ============================================================
# FastAPI와 함께 사용하는 예제
# ============================================================

def main_fastapi():
    """FastAPI + Event-Driven Daemon 통합 예제"""
    
    import ray
    from fastapi import FastAPI
    from contextlib import asynccontextmanager
    import uvicorn
    from sv.backup.event_driven_daemon import EventDrivenDaemon, EventDrivenDaemonConfig
    import logging
    
    # Ray 초기화
    ray.init(num_cpus=4, ignore_reinit_error=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Daemon 생성
    config = EventDrivenDaemonConfig(
        num_executors=3,
        db_path="data/sqlite/jobs.db"
    )
    
    daemon = EventDrivenDaemon(config)
    daemon.register_primary_task(Step1_CollectData())
    daemon.register_secondary_tasks([
        Step2_ProcessItem(),
        Step3_SaveResult()
    ])
    daemon.set_data_splitter(split_items)
    
    # FastAPI 앱 생성
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        logger = logging.getLogger(__name__)
        logger.info("Starting FastAPI + Daemon...")
        daemon.start()
        
        yield  # 앱 실행 중
        
        # Shutdown
        logger.info("Stopping FastAPI + Daemon...")
        daemon.stop()
        ray.shutdown()
    
    app = FastAPI(title="Event-Driven Service", lifespan=lifespan)
    
    # API 엔드포인트
    @app.get("/health")
    async def health():
        return {"status": "ok", "daemon": daemon.get_status()}
    
    @app.post("/jobs")
    async def create_job(frfr_id: str, analysis_id: str):
        job_id = daemon.job_queue.add_job(frfr_id, analysis_id)
        if job_id:
            return {"job_id": job_id, "status": "created"}
        else:
            return {"error": "Failed to create job"}, 400
    
    @app.get("/jobs/{job_id}")
    async def get_job(job_id: int):
        with daemon.job_queue._conn() as conn:
            row = conn.execute(
                "SELECT * FROM job_queue WHERE job_id = ?",
                (job_id,)
            ).fetchone()
            if row:
                return dict(row)
            else:
                return {"error": "Job not found"}, 404
    
    # 실행
    uvicorn.run(app, host="0.0.0.0", port=8090)


# ============================================================
# CLI 사용 예제
# ============================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "fastapi":
        print("\n🚀 FastAPI 모드로 시작")
        main_fastapi()
    else:
        print("\n🚀 Standalone 모드로 시작")
        main_event_driven()

"""
실행 방법:

1. Standalone 모드 (직접 실행):
   $ python sv/daemon_v2_quickstart.py

2. FastAPI 모드:
   $ python sv/daemon_v2_quickstart.py fastapi
   
   그 후 터미널에서:
   $ curl http://localhost:8090/health
   $ curl -X POST "http://localhost:8090/jobs?frfr_id=fire_001&analysis_id=ana_001"
   $ curl http://localhost:8090/jobs/1

기대되는 결과 (로그):
   2024-01-07 10:00:00 - __main__ - INFO - 🚀 Event-Driven Daemon 시작
   2024-01-07 10:00:00 - EventDrivenDaemon - INFO - 🎧 DB Change Listener started
   2024-01-07 10:00:00 - EventDrivenDaemon - INFO - 📊 Fallback Polling started
   2024-01-07 10:00:01 - __main__ - INFO - 📝 Job 생성 중...
   2024-01-07 10:00:01 - __main__ - INFO - ✅ Job 생성됨: job_id=1
   
   (Event triggered)
   
   2024-01-07 10:00:02 - EventDrivenDaemon - INFO - 🔄 Processing job: 1
   2024-01-07 10:00:02 - Executor-0 - INFO - Executor 0 executing with data splitting
   2024-01-07 10:00:02 - Task-collect_data - INFO - Collecting data...
   2024-01-07 10:00:03 - Task-collect_data - INFO - [1] Primary task 'collect_data' completed
   2024-01-07 10:00:03 - Task-collect_data - INFO - [2] Splitting data from primary task result
   2024-01-07 10:00:03 - Task-collect_data - INFO - [2] Data split into 3 items
   
   2024-01-07 10:00:03 - Task-process_item - INFO - [3.1/3.1] Executing task: process_item
   2024-01-07 10:00:04 - Task-process_item - INFO - Processing item 1
   2024-01-07 10:00:05 - Task-save_result - INFO - [3.1/3.1.2] Executing task: save_result
   2024-01-07 10:00:05 - Task-save_result - INFO - Saving result for item 1
   2024-01-07 10:00:05 - Task-save_result - INFO - ✅ Result saved
   
   [Item 2, 3도 동일하게 처리...]
   
   2024-01-07 10:00:20 - EventDrivenDaemon - INFO - ✅ Job 1 completed
   2024-01-07 10:00:20 - __main__ - INFO - ✅ 완료!
"""

