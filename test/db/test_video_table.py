import pytest
from unittest.mock import Mock, MagicMock, patch
from server.backend.db.table.video_table import WildfireVideoTable


@pytest.fixture
def mock_database():
    """Mock SharedDatabase 생성"""
    mock_db = Mock()
    mock_table = MagicMock()
    mock_db.get_table.return_value = mock_table
    return mock_db, mock_table


@pytest.fixture
def video_table(mock_database):
    """WildfireVideoTable 인스턴스 생성"""
    mock_db, _ = mock_database
    with patch('server.backend.db.table.video_table.get_shared_database', return_value=mock_db):
        return WildfireVideoTable(mock_db)


class TestWildfireVideoTableInit:
    """초기화 테스트"""

    def test_init_with_database(self, mock_database):
        """전달된 database로 초기화"""
        mock_db, mock_table = mock_database
        table = WildfireVideoTable(mock_db)

        assert table.db == mock_db
        assert table.table == mock_table
        mock_db.get_table.assert_called_once_with("wildfire_video")

    def test_init_without_database(self):
        """database 없이 초기화 - get_shared_database 호출"""
        mock_db = Mock()
        mock_table = MagicMock()
        mock_db.get_table.return_value = mock_table

        with patch('server.backend.db.table.video_table.get_shared_database', return_value=mock_db):
            table = WildfireVideoTable()

            assert table.db == mock_db
            assert table.table == mock_table


class TestWildfireVideoTableInsert:
    """Insert 메서드 테스트"""

    def test_insert_success(self, video_table, mock_database):
        """비디오 정보 삽입 성공"""
        _, mock_table = mock_database
        mock_table.insert.return_value = 1

        result = video_table.insert(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601",
            video_path="/path/to/video"
        )

        assert result == "1"
        mock_table.insert.assert_called_once_with({
            "frfr_info_id": "fire_001",
            "video_name": "video_001",
            "video_type": "FPA601",
            "video_path": "/path/to/video"
        })

    def test_insert_returns_string_id(self, video_table, mock_database):
        """Insert가 문자열 ID를 반환"""
        _, mock_table = mock_database
        mock_table.insert.return_value = 42

        result = video_table.insert(
            frfr_info_id="fire_002",
            video_name="video_002",
            video_type="FPA630",
            video_path="/path/to/another/video"
        )

        assert isinstance(result, str)
        assert result == "42"


class TestWildfireVideoTableGet:
    """Get 메서드 테스트"""

    def test_get_found(self, video_table, mock_database):
        """비디오 정보 조회 성공"""
        _, mock_table = mock_database
        expected_result = {
            "frfr_info_id": "fire_001",
            "video_name": "video_001",
            "video_type": "FPA601",
            "video_path": "/path/to/video"
        }
        mock_table.get.return_value = expected_result

        result = video_table.get("fire_001", "video_001")

        assert result == expected_result
        mock_table.get.assert_called_once()

    def test_get_not_found(self, video_table, mock_database):
        """비디오 정보 조회 실패"""
        _, mock_table = mock_database
        mock_table.get.return_value = None

        result = video_table.get("fire_999", "video_999")

        assert result is None
        mock_table.get.assert_called_once()

    def test_get_uses_composite_key(self, video_table, mock_database):
        """복합키로 조회"""
        _, mock_table = mock_database
        mock_table.get.return_value = {}

        video_table.get("fire_001", "video_001")

        # 호출된 인자 확인
        call_args = mock_table.get.call_args[0][0]
        assert "fire_001" in str(call_args)
        assert "video_001" in str(call_args)


class TestWildfireVideoTableGetByFrfrId:
    """GetByFrfrId 메서드 테스트"""

    def test_get_by_frfr_id_multiple_results(self, video_table, mock_database):
        """특정 frfr_info_id의 모든 비디오 조회"""
        _, mock_table = mock_database
        expected_results = [
            {
                "frfr_info_id": "fire_001",
                "video_name": "video_001",
                "video_type": "FPA601",
                "video_path": "/path/to/video1"
            },
            {
                "frfr_info_id": "fire_001",
                "video_name": "video_002",
                "video_type": "FPA630",
                "video_path": "/path/to/video2"
            }
        ]
        mock_table.search.return_value = expected_results

        result = video_table.get_by_frfr_id("fire_001")

        assert result == expected_results
        assert len(result) == 2
        mock_table.search.assert_called_once()

    def test_get_by_frfr_id_empty_result(self, video_table, mock_database):
        """조회 결과 없음"""
        _, mock_table = mock_database
        mock_table.search.return_value = []

        result = video_table.get_by_frfr_id("fire_999")

        assert result == []
        mock_table.search.assert_called_once()


class TestWildfireVideoTableUpdate:
    """Update 메서드 테스트"""

    def test_update_success(self, video_table, mock_database):
        """비디오 정보 업데이트 성공"""
        _, mock_table = mock_database
        mock_table.update.return_value = [1]  # 업데이트된 문서 개수

        result = video_table.update(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601_NEW",
            video_path="/new/path"
        )

        assert result is True
        mock_table.update.assert_called_once()

    def test_update_not_found(self, video_table, mock_database):
        """업데이트할 문서 없음"""
        _, mock_table = mock_database
        mock_table.update.return_value = []

        result = video_table.update(
            frfr_info_id="fire_999",
            video_name="video_999",
            video_type="FPA601",
            video_path="/path"
        )

        assert result is False

    def test_update_partial(self, video_table, mock_database):
        """부분 업데이트"""
        _, mock_table = mock_database
        mock_table.update.return_value = [1]

        result = video_table.update(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601_NEW",
            video_path=None
        )

        assert result is True
        # video_path는 포함되지 않아야 함
        call_args = mock_table.update.call_args
        assert "video_path" not in call_args[0][0]

    def test_update_no_data(self, video_table, mock_database):
        """업데이트할 데이터 없음"""
        _, mock_table = mock_database

        result = video_table.update(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type=None,
            video_path=None
        )

        assert result is False
        mock_table.update.assert_not_called()


class TestWildfireVideoTableDelete:
    """Delete 메서드 테스트"""

    def test_delete_success(self, video_table, mock_database):
        """비디오 정보 삭제 성공"""
        _, mock_table = mock_database
        mock_table.remove.return_value = [1]

        result = video_table.delete("fire_001", "video_001")

        assert result is True
        mock_table.remove.assert_called_once()

    def test_delete_not_found(self, video_table, mock_database):
        """삭제할 문서 없음"""
        _, mock_table = mock_database
        mock_table.remove.return_value = []

        result = video_table.delete("fire_999", "video_999")

        assert result is False


class TestWildfireVideoTableDeleteByFrfrId:
    """DeleteByFrfrId 메서드 테스트"""

    def test_delete_by_frfr_id_success(self, video_table, mock_database):
        """특정 frfr_info_id의 모든 비디오 삭제 성공"""
        _, mock_table = mock_database
        mock_table.remove.return_value = [1, 2, 3]  # 3개 문서 삭제

        result = video_table.delete_by_frfr_id("fire_001")

        assert result is True
        mock_table.remove.assert_called_once()

    def test_delete_by_frfr_id_not_found(self, video_table, mock_database):
        """삭제할 문서 없음"""
        _, mock_table = mock_database
        mock_table.remove.return_value = []

        result = video_table.delete_by_frfr_id("fire_999")

        assert result is False


class TestWildfireVideoTableGetAll:
    """GetAll 메서드 테스트"""

    def test_get_all_multiple_results(self, video_table, mock_database):
        """모든 비디오 정보 조회"""
        _, mock_table = mock_database
        expected_results = [
            {
                "frfr_info_id": "fire_001",
                "video_name": "video_001",
                "video_type": "FPA601",
                "video_path": "/path/to/video1"
            },
            {
                "frfr_info_id": "fire_002",
                "video_name": "video_002",
                "video_type": "FPA630",
                "video_path": "/path/to/video2"
            }
        ]
        mock_table.all.return_value = expected_results

        result = video_table.get_all()

        assert result == expected_results
        assert len(result) == 2
        mock_table.all.assert_called_once()

    def test_get_all_empty_result(self, video_table, mock_database):
        """조회 결과 없음"""
        _, mock_table = mock_database
        mock_table.all.return_value = []

        result = video_table.get_all()

        assert result == []
        mock_table.all.assert_called_once()


class TestWildfireVideoTableIntegration:
    """통합 테스트"""

    def test_insert_and_get_flow(self, video_table, mock_database):
        """Insert 후 Get하는 시나리오"""
        _, mock_table = mock_database
        mock_table.insert.return_value = 1

        insert_result = video_table.insert(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601",
            video_path="/path/to/video"
        )

        assert insert_result == "1"
        mock_table.insert.assert_called_once()

    def test_insert_update_delete_flow(self, video_table, mock_database):
        """Insert, Update, Delete 전체 플로우"""
        _, mock_table = mock_database

        # Insert
        mock_table.insert.return_value = 1
        insert_result = video_table.insert(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601",
            video_path="/path"
        )
        assert insert_result == "1"

        # Update
        mock_table.update.return_value = [1]
        update_result = video_table.update(
            frfr_info_id="fire_001",
            video_name="video_001",
            video_type="FPA601_NEW"
        )
        assert update_result is True

        # Delete
        mock_table.remove.return_value = [1]
        delete_result = video_table.delete("fire_001", "video_001")
        assert delete_result is True