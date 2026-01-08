"""
DBConnectionManager와 개선된 DB 구조 테스트
"""
import pytest
import tempfile
import os
from sv.backend.db.connection_manager import DBConnectionManager
from sv.backend.db.job_queue_db import JobQueue, JobStatus
from sv.backend.db.task_db import TaskQueue
from sv.backend.service.job_queue_service import JobQueueService


class TestDBConnectionManager:
    """DBConnectionManager Singleton 동작 테스트"""
    
    def test_singleton_same_path(self):
        """같은 db_path를 사용하면 같은 인스턴스를 반환해야 함"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            manager1 = DBConnectionManager(db_path)
            manager2 = DBConnectionManager(db_path)
            
            # 같은 인스턴스여야 함
            assert manager1 is manager2
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_singleton_different_path(self):
        """다른 db_path를 사용하면 다른 인스턴스를 반환해야 함"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp1:
            db_path1 = tmp1.name
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp2:
            db_path2 = tmp2.name
        
        try:
            manager1 = DBConnectionManager(db_path1)
            manager2 = DBConnectionManager(db_path2)
            
            # 다른 인스턴스여야 함
            assert manager1 is not manager2
        finally:
            if os.path.exists(db_path1):
                os.unlink(db_path1)
            if os.path.exists(db_path2):
                os.unlink(db_path2)
    
    def test_table_initialization_tracking(self):
        """테이블 초기화 상태 추적 테스트"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            manager = DBConnectionManager(db_path)
            
            # 초기에는 초기화되지 않음
            assert not manager.is_table_initialized("test_table")
            
            # 초기화 표시
            manager.mark_table_initialized("test_table")
            
            # 초기화됨
            assert manager.is_table_initialized("test_table")
            
            # 다른 테이블은 여전히 초기화 안됨
            assert not manager.is_table_initialized("other_table")
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


class TestSharedConnection:
    """JobQueue와 TaskQueue가 DB 연결을 공유하는지 테스트"""
    
    def test_shared_connection_manager(self):
        """JobQueue와 TaskQueue가 같은 ConnectionManager를 사용해야 함"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            job_queue = JobQueue(db_path)
            task_queue = TaskQueue(db_path)
            
            # 같은 ConnectionManager를 사용해야 함
            assert job_queue.connection_manager is task_queue.connection_manager
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_table_init_called_once(self):
        """테이블 초기화가 한 번만 호출되어야 함"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # 첫 번째 JobQueue 생성 및 초기화
            job_queue1 = JobQueue(db_path)
            job_queue1._init_db()
            
            # ConnectionManager에 초기화 표시되어야 함
            assert job_queue1.connection_manager.is_table_initialized("job_queue")
            
            # 두 번째 JobQueue 생성 및 초기화 시도
            job_queue2 = JobQueue(db_path)
            job_queue2._init_db()  # skip되어야 함
            
            # 여전히 같은 ConnectionManager
            assert job_queue1.connection_manager is job_queue2.connection_manager
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


class TestJobQueueServiceNonSingleton:
    """JobQueueService가 Singleton이 아니어도 작동하는지 테스트"""
    
    def test_multiple_service_instances_share_data(self):
        """여러 JobQueueService 인스턴스가 데이터를 공유해야 함"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # 첫 번째 서비스로 job 추가
            service1 = JobQueueService(db_path)
            job_id = service1.add_job("fire_001", "analysis_001")
            assert job_id is not None
            
            # 두 번째 서비스로 조회
            service2 = JobQueueService(db_path)
            jobs = service2.get_all_jobs()
            
            # 첫 번째 서비스에서 추가한 job이 보여야 함
            assert len(jobs) == 1
            assert jobs[0]['frfr_id'] == "fire_001"
            assert jobs[0]['analysis_id'] == "analysis_001"
            
            # 두 서비스는 다른 인스턴스
            assert service1 is not service2
            
            # 하지만 같은 ConnectionManager를 공유
            assert service1.job_queue.connection_manager is service2.job_queue.connection_manager
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_service_with_tasks(self):
        """JobQueueService의 task 관련 기능 테스트"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            service = JobQueueService(db_path)
            
            # Job과 Task 추가
            job_id = service.add_job_with_tasks(
                "fire_002", 
                "analysis_002",
                ["task1", "task2", "task3"]
            )
            assert job_id is not None
            
            # Task 조회
            tasks = service.get_job_tasks(job_id)
            assert len(tasks) == 3
            
            # 새로운 서비스 인스턴스로도 조회 가능
            service2 = JobQueueService(db_path)
            tasks2 = service2.get_job_tasks(job_id)
            assert len(tasks2) == 3
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


class TestConcurrency:
    """멀티스레드 환경에서의 동작 테스트"""
    
    def test_multiple_threads(self):
        """여러 스레드에서 동시에 접근해도 안전해야 함"""
        from threading import Thread
        import time
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        results = []
        
        def worker(thread_id):
            try:
                service = JobQueueService(db_path)
                job_id = service.add_job(f"fire_{thread_id}", f"analysis_{thread_id}")
                results.append(job_id)
            except Exception as e:
                results.append(f"Error: {str(e)}")
        
        try:
            # 10개의 스레드 생성
            threads = [Thread(target=worker, args=(i,)) for i in range(10)]
            
            # 모든 스레드 시작
            for t in threads:
                t.start()
            
            # 모든 스레드 종료 대기
            for t in threads:
                t.join()
            
            # 모든 job이 성공적으로 추가되었는지 확인
            assert len(results) == 10
            assert all(isinstance(r, int) for r in results)
            
            # 최종 확인
            service = JobQueueService(db_path)
            jobs = service.get_all_jobs()
            assert len(jobs) == 10
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

