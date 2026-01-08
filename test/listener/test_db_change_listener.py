"""
DBChangeListener 테스트

서비스 레이어 통합 테스트
"""

import unittest
from unittest.mock import Mock, patch
from sv.daemon.module.db_change_listener import (
    DBChangeListener, 
    DBChangeEvent, 
    ChangeEventType
)


class TestDBChangeListener(unittest.TestCase):
    """DBChangeListener 테스트 케이스"""
    
    def setUp(self):
        """각 테스트 전 설정"""
        self.mock_service = Mock()
        self.listener = DBChangeListener(
            job_queue_service=self.mock_service,
            poll_interval=1.0
        )
        self.callback = Mock()
        self.listener.on_change(self.callback)
    
    def test_initialization(self):
        """리스너 초기화 테스트"""
        self.assertIsNotNone(self.listener.job_queue_service)
        self.assertEqual(self.listener.poll_interval, 1.0)
        self.assertEqual(len(self.listener.callbacks), 1)
    
    def test_check_pending_jobs_detected(self):
        """Pending Job 감지 테스트"""
        # Mock 데이터 설정
        mock_jobs = [
            {"job_id": 1, "frfr_id": "FIRE_001", "status": "pending"},
            {"job_id": 2, "frfr_id": "FIRE_002", "status": "pending"},
        ]
        self.mock_service.get_jobs_by_status.return_value = mock_jobs
        
        # Pending Job 확인
        events = self.listener.check_pending_jobs(status="pending")
        
        # 검증
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, ChangeEventType.PENDING_JOBS_DETECTED)
        self.assertEqual(events[0].data["count"], 2)
        self.assertEqual(len(events[0].data["jobs"]), 2)
        
        # 콜백 호출 확인
        self.callback.assert_called_once()
    
    def test_check_pending_jobs_no_jobs(self):
        """Pending Job 없음 테스트"""
        # Mock 데이터 설정 (빈 리스트)
        self.mock_service.get_jobs_by_status.return_value = []
        
        # Pending Job 확인
        events = self.listener.check_pending_jobs(status="pending")
        
        # 검증 - 이벤트 없음
        self.assertEqual(len(events), 0)
        self.callback.assert_not_called()
    
    def test_check_pending_jobs_count_change(self):
        """Pending Job 개수 변경 감지 테스트"""
        # 첫 번째 호출 - 2개 작업
        self.mock_service.get_jobs_by_status.return_value = [
            {"job_id": 1, "frfr_id": "FIRE_001", "status": "pending"},
            {"job_id": 2, "frfr_id": "FIRE_002", "status": "pending"},
        ]
        
        events1 = self.listener.check_pending_jobs(status="pending")
        self.assertEqual(len(events1), 1)
        self.assertEqual(events1[0].data["count"], 2)
        
        # 두 번째 호출 - 3개 작업 (개수 증가)
        self.mock_service.get_jobs_by_status.return_value = [
            {"job_id": 1, "frfr_id": "FIRE_001", "status": "pending"},
            {"job_id": 2, "frfr_id": "FIRE_002", "status": "pending"},
            {"job_id": 3, "frfr_id": "FIRE_003", "status": "pending"},
        ]
        
        events2 = self.listener.check_pending_jobs(status="pending")
        self.assertEqual(len(events2), 1)
        self.assertEqual(events2[0].data["count"], 3)
    
    def test_has_pending_jobs_true(self):
        """Pending Job 존재 확인 - True 케이스"""
        self.mock_service.has_pending_jobs.return_value = True
        
        result = self.listener.has_pending_jobs()
        
        self.assertTrue(result)
        self.mock_service.has_pending_jobs.assert_called_once()
    
    def test_has_pending_jobs_false(self):
        """Pending Job 존재 확인 - False 케이스"""
        self.mock_service.has_pending_jobs.return_value = False
        
        result = self.listener.has_pending_jobs()
        
        self.assertFalse(result)
    
    def test_count_jobs_by_status(self):
        """상태별 Job 개수 조회 테스트"""
        self.mock_service.count_jobs_by_status.return_value = 5
        
        # 상태 확인
        with patch('sv.backend.db.job_queue.JobStatus') as MockJobStatus:
            MockJobStatus.side_effect = lambda x: x
            count = self.listener.check_jobs_by_count(status="pending")
        
        self.assertEqual(count, 5)
    
    def test_callback_error_handling(self):
        """콜백 에러 처리 테스트"""
        # 에러 발생하는 콜백 추가
        error_callback = Mock(side_effect=Exception("Test error"))
        self.listener.callbacks.append(error_callback)
        
        # Mock 데이터 설정
        self.mock_service.get_jobs_by_status.return_value = [
            {"job_id": 1, "frfr_id": "FIRE_001", "status": "pending"}
        ]
        
        # 에러가 발생해도 다른 콜백은 계속 실행되어야 함
        events = self.listener.check_pending_jobs(status="pending")
        
        self.assertEqual(len(events), 1)
        # 두 콜백 모두 호출되어야 함 (에러가 발생하더라도)
        error_callback.assert_called_once()
        self.callback.assert_called_once()


class TestDBChangeEvent(unittest.TestCase):
    """DBChangeEvent 테스트"""
    
    def test_event_creation(self):
        """이벤트 생성 테스트"""
        data = {"status": "pending", "count": 2}
        event = DBChangeEvent(
            table_name="job_queue",
            event_type=ChangeEventType.PENDING_JOBS_DETECTED,
            data=data
        )
        
        self.assertEqual(event.table_name, "job_queue")
        self.assertEqual(event.event_type, ChangeEventType.PENDING_JOBS_DETECTED)
        self.assertEqual(event.data, data)
        self.assertIsNotNone(event.timestamp)
    
    def test_event_repr(self):
        """이벤트 문자열 표현 테스트"""
        event = DBChangeEvent(
            table_name="job_queue",
            event_type=ChangeEventType.PENDING_JOBS_DETECTED,
            data={"count": 2}
        )
        
        repr_str = repr(event)
        self.assertIn("job_queue", repr_str)
        self.assertIn("pending_jobs_detected", repr_str)


if __name__ == "__main__":
    unittest.main()

