import unittest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

from server.backend.db.database import SharedDatabase, get_shared_database


class TestSharedDatabase(unittest.TestCase):
    """SharedDatabase 클래스 테스트"""

    def setUp(self):
        """테스트 실행 전 임시 디렉토리 생성"""
        self.test_dir = tempfile.mkdtemp()
        self.test_db_path = Path(self.test_dir) / "test_wildfire.json"

    def tearDown(self):
        """테스트 실행 후 임시 디렉토리 정리"""
        if self.test_db_path.exists():
            self.test_db_path.unlink()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_shared_database_initialization(self):
        """SharedDatabase 초기화 테스트"""
        db = SharedDatabase(self.test_db_path)

        self.assertIsNotNone(db.db)
        self.assertTrue(self.test_db_path.exists())

    def test_shared_database_creates_parent_directory(self):
        """부모 디렉토리가 없을 때 생성되는지 확인"""
        nested_path = Path(self.test_dir) / "nested" / "path" / "test.json"

        db = SharedDatabase(nested_path)

        self.assertTrue(nested_path.parent.exists())
        self.assertTrue(nested_path.exists())
        db.close()

    def test_get_table(self):
        """테이블 획득 테스트"""
        db = SharedDatabase(self.test_db_path)
        table = db.get_table("test_table")

        self.assertIsNotNone(table)
        db.close()

    def test_get_table_multiple_tables(self):
        """여러 테이블 획득 테스트"""
        db = SharedDatabase(self.test_db_path)

        table1 = db.get_table("table1")
        table2 = db.get_table("table2")

        self.assertIsNotNone(table1)
        self.assertIsNotNone(table2)
        db.close()

    def test_database_close(self):
        """데이터베이스 연결 종료 테스트"""
        db = SharedDatabase(self.test_db_path)
        db.close()

        # 폐쇄된 데이터베이스는 접근할 수 없음
        with self.assertRaises(Exception):
            db.db.tables()

    def test_insert_and_retrieve(self):
        """데이터 삽입 및 조회 테스트"""
        db = SharedDatabase(self.test_db_path)
        table = db.get_table("test_data")

        # 데이터 삽입
        doc_id = table.insert({
            "name": "test",
            "value": 123
        })

        self.assertIsNotNone(doc_id)

        # 데이터 조회
        doc = table.get(doc_id=doc_id)
        self.assertEqual(doc["name"], "test")
        self.assertEqual(doc["value"], 123)

        db.close()

    def test_persistence(self):
        """데이터 지속성 테스트"""
        db1 = SharedDatabase(self.test_db_path)
        table1 = db1.get_table("persistent_data")
        table1.insert({"id": 1, "data": "test"})
        db1.close()

        # 새 인스턴스로 같은 데이터베이스 열기
        db2 = SharedDatabase(self.test_db_path)
        table2 = db2.get_table("persistent_data")
        results = table2.all()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["data"], "test")

        db2.close()


class TestGetSharedDatabase(unittest.TestCase):
    """get_shared_database 함수 테스트"""

    def setUp(self):
        """테스트 전 싱글톤 초기화"""
        # 싱글톤 인스턴스 리셋
        import server.backend.db.database as db_module
        db_module._shared_db_instance = None
        self.db_module = db_module

    def tearDown(self):
        """테스트 후 정리"""
        if self.db_module._shared_db_instance:
            self.db_module._shared_db_instance.close()
        self.db_module._shared_db_instance = None

    def test_get_shared_database_singleton(self):
        """싱글톤 패턴 확인"""
        db1 = get_shared_database()
        db2 = get_shared_database()

        self.assertIs(db1, db2)

    def test_get_shared_database_returns_shared_database_instance(self):
        """반환 타입 확인"""
        db = get_shared_database()

        self.assertIsInstance(db, SharedDatabase)

    def test_get_shared_database_multiple_calls(self):
        """여러 번 호출해도 같은 인스턴스"""
        instances = [get_shared_database() for _ in range(5)]

        for instance in instances[1:]:
            self.assertIs(instances[0], instance)


class TestVideoTableWithRealDatabase(unittest.TestCase):
    """실제 SharedDatabase와 함께 VideoTable 테스트"""

    def setUp(self):
        """테스트 실행 전 임시 데이터베이스 생성"""
        self.test_dir = tempfile.mkdtemp()
        self.test_db_path = Path(self.test_dir) / "test_wildfire.json"
        self.db = SharedDatabase(self.test_db_path)

    def tearDown(self):
        """테스트 실행 후 정리"""
        self.db.close()
        if self.test_db_path.exists():
            self.test_db_path.unlink()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_video_table_insert_and_get(self):
        """VideoTable Insert 및 Get 테스트"""
        from server.backend.db.table.video_table import WildfireVideoTable

        video_table = WildfireVideoTable(self.db)

        # Insert
        doc_id = video_table.insert(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601",
            video_path="/path/to/video"
        )

        self.assertIsNotNone(doc_id)
        self.assertIsInstance(doc_id, str)

        # Get
        result = video_table.get("fire_001", "video_001")

        self.assertIsNotNone(result)
        self.assertEqual(result["frfr_info_id"], "fire_001")
        self.assertEqual(result["video_name"], "video_001")
        self.assertEqual(result["video_type"], "FPA601")
        self.assertEqual(result["video_path"], "/path/to/video")

    def test_video_table_insert_multiple_and_get_by_frfr_id(self):
        """VideoTable 다중 Insert 및 GetByFrfrId 테스트"""
        from server.backend.db.table.video_table import WildfireVideoTable

        video_table = WildfireVideoTable(self.db)

        # 같은 fire_id로 여러 비디오 삽입
        for i in range(3):
            video_table.insert(
                frfr_info_id="fire_001",
                video_name=f"video_{i:03d}",
                video_type="FPA601" if i % 2 == 0 else "FPA630",
                video_path=f"/path/to/video_{i}"
            )

        # Get by frfr_id
        results = video_table.get_by_frfr_id("fire_001")

        self.assertEqual(len(results), 3)
        for result in results:
            self.assertEqual(result["frfr_info_id"], "fire_001")

    def test_video_table_update(self):
        """VideoTable Update 테스트"""
        from server.backend.db.table.video_table import WildfireVideoTable

        video_table = WildfireVideoTable(self.db)

        # Insert
        video_table.insert(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601",
            video_path="/original/path"
        )

        # Update
        success = video_table.update(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA630",
            video_path="/updated/path"
        )

        self.assertTrue(success)

        # Verify
        result = video_table.get("fire_001", "video_001")
        self.assertEqual(result["video_type"], "FPA630")
        self.assertEqual(result["video_path"], "/updated/path")

    def test_video_table_delete(self):
        """VideoTable Delete 테스트"""
        from server.backend.db.table.video_table import WildfireVideoTable

        video_table = WildfireVideoTable(self.db)

        # Insert
        video_table.insert(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601",
            video_path="/path/to/video"
        )

        # Delete
        success = video_table.delete("fire_001", "video_001")
        self.assertTrue(success)

        # Verify
        result = video_table.get("fire_001", "video_001")
        self.assertIsNone(result)

    def test_video_table_delete_by_frfr_id(self):
        """VideoTable DeleteByFrfrId 테스트"""
        from server.backend.db.table.video_table import WildfireVideoTable

        video_table = WildfireVideoTable(self.db)

        # 같은 fire_id로 여러 비디오 삽입
        for i in range(3):
            video_table.insert(
                frfr_info_id="fire_001",
                video_name=f"video_{i:03d}",
                video_type="FPA601",
                video_path=f"/path/to/video_{i}"
            )

        # Delete by frfr_id
        success = video_table.delete_by_frfr_id("fire_001")
        self.assertTrue(success)

        # Verify
        results = video_table.get_by_frfr_id("fire_001")
        self.assertEqual(len(results), 0)

    def test_video_table_get_all(self):
        """VideoTable GetAll 테스트"""
        from server.backend.db.table.video_table import WildfireVideoTable

        video_table = WildfireVideoTable(self.db)

        # 여러 fire_id로 비디오 삽입
        for fire_id in ["fire_001", "fire_002"]:
            for i in range(2):
                video_table.insert(
                    frfr_info_id=fire_id,
                    video_name=f"video_{i:03d}",
                    video_type="FPA601",
                    video_path=f"/path/{fire_id}/video_{i}"
                )

        # Get all
        results = video_table.get_all()

        self.assertEqual(len(results), 4)

    def test_video_table_full_workflow(self):
        """VideoTable 전체 워크플로우 테스트"""
        from server.backend.db.table.video_table import WildfireVideoTable

        video_table = WildfireVideoTable(self.db)

        # 1. Insert
        doc_id = video_table.insert(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601",
            video_path="/original/path"
        )
        self.assertIsNotNone(doc_id)

        # 2. Get
        result = video_table.get("fire_001", "video_001")
        self.assertIsNotNone(result)
        self.assertEqual(result["video_type"], "FPA601")

        # 3. Get All
        all_results = video_table.get_all()
        self.assertEqual(len(all_results), 1)

        # 4. Update
        success = video_table.update(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA630"
        )
        self.assertTrue(success)

        # 5. Verify Update
        result = video_table.get("fire_001", "video_001")
        self.assertEqual(result["video_type"], "FPA630")

        # 6. Delete
        success = video_table.delete("fire_001", "video_001")
        self.assertTrue(success)

        # 7. Verify Delete
        result = video_table.get("fire_001", "video_001")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()