"""
Event-Driven Daemon 사용 예제

예시 시나리오:
1. Task1: 데이터베이스에서 분석할 비디오 목록 조회
2. Task2: 각 비디오에 대해 AI 분석 수행
3. Task3: 분석 결과를 데이터베이스에 저장

Flow:
DB Update (new job) 
  → Event Listener 감지 
    → Task1 실행 (비디오 목록 조회) 
      → 결과 분할 (각 비디오별로) 
        → 각 비디오에 대해 Task2, Task3 순차 실행
"""

import logging
from typing import Dict, Any, List
import time

from sv.task.task_base import TaskBase
from sv.backup.event_driven_daemon import EventDrivenDaemon, EventDrivenDaemonConfig
from fastapi import FastAPI
import uvicorn
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


# ==================== Task 구현 예제 ====================

class FetchVideoListTask(TaskBase):
    """Task 1: 분석할 비디오 목록 조회"""
    
    def __init__(self):
        super().__init__("fetch_video_list")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        데이터베이스에서 PENDING 상태의 비디오 목록 조회
        
        Args:
            context: 이전 Task 결과
            
        Returns:
            비디오 목록
        """
        self.logger.info(f"Fetching video list... (Job ID: {context.get('job_id', 'N/A')})")
        
        # 시뮬레이션: DB에서 데이터 조회
        time.sleep(1)
        
        # 실제로는 여기서 DB 쿼리 수행
        videos = [
            {
                'video_id': 'vid_001',
                'file_path': '/data/videos/fire_001.mp4',
                'location': 'Seoul',
                'timestamp': '2024-01-01T10:00:00'
            },
            {
                'video_id': 'vid_002',
                'file_path': '/data/videos/fire_002.mp4',
                'location': 'Busan',
                'timestamp': '2024-01-01T11:00:00'
            },
            {
                'video_id': 'vid_003',
                'file_path': '/data/videos/fire_003.mp4',
                'location': 'Incheon',
                'timestamp': '2024-01-01T12:00:00'
            }
        ]
        
        self.logger.info(f"Fetched {len(videos)} videos")
        
        return {
            'status': 'success',
            'videos': videos,
            'count': len(videos)
        }


class AnalyzeVideoTask(TaskBase):
    """Task 2: 각 비디오 분석"""
    
    def __init__(self):
        super().__init__("analyze_video")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        비디오에 대해 AI 분석 수행
        
        Args:
            context: 현재 데이터 아이템 포함하는 컨텍스트
            
        Returns:
            분석 결과
        """
        video = context.get('current_data', {})
        video_id = video.get('video_id', 'unknown')
        
        self.logger.info(f"Analyzing video: {video_id}")
        
        # 시뮬레이션: AI 분석 수행
        time.sleep(2)
        
        # 실제로는 여기서 AI 모델 실행
        analysis_result = {
            'video_id': video_id,
            'fire_detected': True,
            'confidence': 0.95,
            'affected_area': 45.2,  # hectares
            'frame_count': 1200
        }
        
        self.logger.info(f"Analysis complete for {video_id}: confidence={analysis_result['confidence']}")
        
        return {
            'status': 'success',
            'analysis': analysis_result
        }


class SaveAnalysisResultTask(TaskBase):
    """Task 3: 분석 결과 저장"""
    
    def __init__(self):
        super().__init__("save_analysis_result")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        분석 결과를 데이터베이스에 저장
        
        Args:
            context: 현재 데이터 아이템과 이전 Task 결과 포함하는 컨텍스트
            
        Returns:
            저장 결과
        """
        video = context.get('current_data', {})
        analysis_result = context.get('analyze_video', {}).get('analysis', {})
        
        video_id = video.get('video_id', 'unknown')
        
        self.logger.info(f"Saving analysis result for video: {video_id}")
        
        # 시뮬레이션: DB 저장
        time.sleep(0.5)
        
        # 실제로는 여기서 DB에 INSERT/UPDATE
        saved_data = {
            'video_id': video_id,
            'analysis_id': f"ana_{video_id}_{int(time.time())}",
            'saved_at': '2024-01-01T13:00:00',
            'status': 'completed'
        }
        
        self.logger.info(f"Analysis result saved for {video_id}")
        
        return {
            'status': 'success',
            'saved': saved_data
        }


# ==================== 데이터 분할 함수 ====================

def split_videos(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Task 1의 결과에서 비디오 리스트를 분할
    
    Args:
        result: Task 1의 실행 결과
        
    Returns:
        개별 비디오 목록
    """
    videos = result.get('videos', [])
    logger.info(f"Splitting {len(videos)} videos for parallel processing")
    return videos


# ==================== FastAPI + Daemon 통합 ====================

# Daemon 인스턴스 생성
config = EventDrivenDaemonConfig(
    num_executors=3,
    db_path="data/sqlite/jobs.db",
    poll_interval=2.0,
    fallback_poll_interval=30,
    enable_event_listener=True,
    enable_fallback_polling=True
)

daemon = EventDrivenDaemon(config)

# Task 등록
daemon.register_primary_task(FetchVideoListTask())
daemon.register_secondary_tasks([
    AnalyzeVideoTask(),
    SaveAnalysisResultTask()
])
daemon.set_data_splitter(split_videos)


# 커스텀 DB 변경 콜백 (선택사항)
def custom_db_change_handler(event):
    """DB 변경 감지 시 추가 처리"""
    logger.info(f"Custom handler: {event.table_name} {event.event_type.value}")


# FastAPI 앱 생성
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI 생명주기"""
    # Startup
    logger.info("Starting FastAPI app with Event-Driven Daemon...")
    daemon.start()
    
    yield  # 앱 실행 중
    
    # Shutdown
    logger.info("Shutting down FastAPI app...")
    daemon.stop()


app = FastAPI(
    title="F-Line Service (Event-Driven)",
    description="야산불 감지 서비스 - 이벤트 기반 Daemon",
    lifespan=lifespan
)


# ==================== API Endpoints ====================

@app.get("/health")
async def health_check():
    """헬스 체크"""
    return {
        'status': 'healthy',
        'daemon': daemon.get_status()
    }


@app.post("/api/jobs")
async def create_job(frfr_id: str, analysis_id: str):
    """
    새로운 Job 생성
    
    Args:
        frfr_id: 산불 정보 ID
        analysis_id: 분석 ID
        
    Returns:
        생성된 Job 정보
    """
    try:
        job_id = daemon.job_queue.add_job(frfr_id, analysis_id)
        
        if job_id:
            return {
                'status': 'created',
                'job_id': job_id,
                'frfr_id': frfr_id,
                'analysis_id': analysis_id
            }
        else:
            return {
                'status': 'error',
                'message': 'Failed to create job (duplicate or error)'
            }, 400
    
    except Exception as e:
        logger.error(f"Error creating job: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }, 500


@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: int):
    """
    Job 상태 조회
    
    Args:
        job_id: Job ID
        
    Returns:
        Job 상태 정보
    """
    try:
        with daemon.job_queue._conn() as conn:
            row = conn.execute(
                "SELECT job_id, frfr_id, analysis_id, status, created_at FROM job_queue WHERE job_id = ?",
                (job_id,)
            ).fetchone()
            
            if row:
                return {
                    'job_id': row['job_id'],
                    'frfr_id': row['frfr_id'],
                    'analysis_id': row['analysis_id'],
                    'status': row['status'],
                    'created_at': row['created_at']
                }
            else:
                return {
                    'status': 'error',
                    'message': f'Job {job_id} not found'
                }, 404
    
    except Exception as e:
        logger.error(f"Error getting job status: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }, 500


@app.get("/api/daemon/status")
async def daemon_status():
    """Daemon 상태 조회"""
    return daemon.get_status()


# ==================== 메인 ====================

if __name__ == '__main__':
    import ray
    
    # Ray 초기화
    ray.init(num_cpus=8, ignore_reinit_error=True)
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("🚀 Starting F-Line Service (Event-Driven Daemon)")
    logger.info("=" * 80)
    
    # Uvicorn으로 FastAPI 앱 실행
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8090,
        log_level="info"
    )

