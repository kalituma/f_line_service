"""
TinyDB 백엔드 테스트 스크립트 (Wildfire + Wildfire Video)
"""

from server.backend.service import get_wildfire_service
import json


def test_create_and_retrieve():
    """데이터 생성 및 조회 테스트"""
    print("\n=== 테스트 1: 데이터 생성 및 조회 ===")

    service = get_wildfire_service()

    # 데이터 생성
    response = service.create_wildfire(
        frfr_info_id="TEST_001",
        analysis_id="20251221_TEST_001",
        fire_location={"latitude": 37.5, "longitude": 127.0},
        videos=[
            {
                "path": "/data/videos/test1.mkv",
                "name": "test1.mkv",
                "add_time": "2025-12-21 10:00:00",
                "analysis_status": "STAT_001",
            },
            {
                "path": "/data/videos/test2.mkv",
                "name": "test2.mkv",
                "add_time": "2025-12-21 10:05:00",
                "analysis_status": "STAT_001",
            },
        ],
    )

    print(f"✅ 데이터 생성 완료")
    print(f"   ID: {response.frfr_info_id}")
    print(f"   위치: {response.fire_location.latitude}, {response.fire_location.longitude}")
    print(f"   비디오 수: {len(response.videos)}")

    # 데이터 조회
    retrieved = service.get_wildfire("TEST_001")
    print(f"\n✅ 데이터 조회 완료")
    print(f"   ID: {retrieved.frfr_info_id}")
    print(f"   분석ID: {retrieved.analysis_id}")
    print(f"   위치: {retrieved.fire_location.latitude}, {retrieved.fire_location.longitude}")

    # 비디오 정보 출력
    print(f"\n   비디오 목록:")
    for i, video in enumerate(retrieved.videos, 1):
        print(f"   {i}. {video.name} ({video.path})")

    return response


def test_update():
    """데이터 업데이트 테스트"""
    print("\n=== 테스트 2: 데이터 업데이트 ===")

    service = get_wildfire_service()

    # 위치 정보 업데이트
    updated = service.update_wildfire(
        frfr_info_id="TEST_001",
        fire_location={"latitude": 35.0, "longitude": 129.0},
    )

    print(f"✅ 위치 정보 업데이트 완료")
    print(f"   새로운 위치: {updated.fire_location.latitude}, {updated.fire_location.longitude}")


def test_add_video():
    """비디오 추가 테스트"""
    print("\n=== 테스트 3: 비디오 추가 ===")

    service = get_wildfire_service()

    # 비디오 추가
    updated = service.add_video_to_wildfire(
        frfr_info_id="TEST_001",
        video_info={
            "path": "/data/videos/test3.mkv",
            "name": "test3.mkv",
            "add_time": "2025-12-21 10:10:00",
            "analysis_status": "STAT_001",
        },
    )

    print(f"✅ 비디오 추가 완료")
    print(f"   총 비디오 수: {len(updated.videos)}")
    for i, video in enumerate(updated.videos, 1):
        print(f"   {i}. {video.name}")


def test_list_all():
    """모든 데이터 조회 테스트"""
    print("\n=== 테스트 4: 모든 데이터 조회 ===")

    service = get_wildfire_service()

    # 모든 데이터 조회
    all_wildfires = service.get_all_wildfires()

    print(f"✅ 총 {len(all_wildfires)}개의 산불 기록 조회")
    for i, wildfire in enumerate(all_wildfires, 1):
        print(f"\n   {i}. ID: {wildfire.frfr_info_id}")
        print(f"      분석ID: {wildfire.analysis_id}")
        print(f"      비디오 수: {len(wildfire.videos)}")


def test_remove_video():
    """비디오 제거 테스트"""
    print("\n=== 테스트 5: 비디오 제거 ===")

    service = get_wildfire_service()

    # 비디오 제거 전 조회
    before = service.get_wildfire("TEST_001")
    print(f"제거 전 비디오 수: {len(before.videos)}")

    # 비디오 제거
    updated = service.remove_video_from_wildfire(
        frfr_info_id="TEST_001",
        video_url="/data/videos/test3.mkv",
    )

    print(f"✅ 비디오 제거 완료")
    print(f"제거 후 비디오 수: {len(updated.videos)}")
    for i, video in enumerate(updated.videos, 1):
        print(f"   {i}. {video.name}")


def test_delete():
    """데이터 삭제 테스트"""
    print("\n=== 테스트 6: 데이터 삭제 ===")

    service = get_wildfire_service()

    # 삭제 전 데이터 수 조회
    all_before = service.get_all_wildfires()
    print(f"삭제 전 총 기록: {len(all_before)}")

    # 데이터 삭제
    success = service.delete_wildfire("TEST_001")

    if success:
        print(f"✅ 데이터 삭제 완료")
        all_after = service.get_all_wildfires()
        print(f"삭제 후 총 기록: {len(all_after)}")
    else:
        print(f"❌ 데이터 삭제 실패")


def test_create_wildfire_video():
    """비디오 테이블 생성 테스트"""
    print("\n=== 테스트 7: Wildfire Video 생성 ===")

    service = get_wildfire_service()

    # 비디오 생성
    video1 = service.create_wildfire_video(
        frfr_info_id="VIDEO_TEST_001",
        video_name="FPA601_20251221_100000_1234_000.mkv",
        video_type="FPA601",
        video_path="/data/helivid/hv_proc/VIDEO_TEST_001/FPA601/FPA601_20251221_100000_1234_000.mkv",
    )

    print(f"✅ 비디오 생성 완료 #1")
    print(f"   ID: {video1.frfr_info_id}")
    print(f"   이름: {video1.video_name}")
    print(f"   타입: {video1.video_type}")
    print(f"   경로: {video1.video_path}")

    # 두 번째 비디오 생성
    video2 = service.create_wildfire_video(
        frfr_info_id="VIDEO_TEST_001",
        video_name="FPA630_20251221_100100_5678_000.mkv",
        video_type="FPA630",
        video_path="/data/helivid/hv_proc/VIDEO_TEST_001/FPA630/FPA630_20251221_100100_5678_000.mkv",
    )

    print(f"\n✅ 비디오 생성 완료 #2")
    print(f"   ID: {video2.frfr_info_id}")
    print(f"   이름: {video2.video_name}")
    print(f"   타입: {video2.video_type}")


def test_get_wildfire_video():
    """비디오 조회 테스트"""
    print("\n=== 테스트 8: Wildfire Video 조회 (복합키) ===")

    service = get_wildfire_service()

    # 특정 비디오 조회
    video = service.get_wildfire_video(
        frfr_info_id="VIDEO_TEST_001",
        video_name="FPA601_20251221_100000_1234_000.mkv",
    )

    if video:
        print(f"✅ 비디오 조회 완료")
        print(f"   ID: {video.frfr_info_id}")
        print(f"   이름: {video.video_name}")
        print(f"   타입: {video.video_type}")
        print(f"   경로: {video.video_path}")
    else:
        print(f"❌ 비디오를 찾을 수 없습니다")


def test_get_wildfire_videos_by_id():
    """특정 ID의 모든 비디오 조회 테스트"""
    print("\n=== 테스트 9: Wildfire Videos 목록 조회 ===")

    service = get_wildfire_service()

    # 모든 비디오 조회
    videos = service.get_wildfire_videos(frfr_info_id="VIDEO_TEST_001")

    print(f"✅ 총 {len(videos)}개의 비디오 조회 완료")
    for i, video in enumerate(videos, 1):
        print(f"   {i}. {video.video_name} ({video.video_type})")
        print(f"      경로: {video.video_path}")


def test_update_wildfire_video():
    """비디오 업데이트 테스트"""
    print("\n=== 테스트 10: Wildfire Video 업데이트 ===")

    service = get_wildfire_service()

    # 비디오 업데이트
    updated = service.update_wildfire_video(
        frfr_info_id="VIDEO_TEST_001",
        video_name="FPA601_20251221_100000_1234_000.mkv",
        video_type="FPA601_UPDATED",
        video_path="/data/helivid/updated/FPA601_20251221_100000_1234_000.mkv",
    )

    if updated:
        print(f"✅ 비디오 업데이트 완료")
        print(f"   새로운 타입: {updated.video_type}")
        print(f"   새로운 경로: {updated.video_path}")
    else:
        print(f"❌ 비디오 업데이트 실패")


def test_delete_wildfire_video():
    """비디오 삭제 테스트"""
    print("\n=== 테스트 11: Wildfire Video 삭제 ===")

    service = get_wildfire_service()

    # 삭제 전 조회
    before = service.get_wildfire_videos(frfr_info_id="VIDEO_TEST_001")
    print(f"삭제 전 비디오 수: {len(before)}")

    # 비디오 삭제
    success = service.delete_wildfire_video(
        frfr_info_id="VIDEO_TEST_001",
        video_name="FPA630_20251221_100100_5678_000.mkv",
    )

    if success:
        print(f"✅ 비디오 삭제 완료")
        after = service.get_wildfire_videos(frfr_info_id="VIDEO_TEST_001")
        print(f"삭제 후 비디오 수: {len(after)}")
    else:
        print(f"❌ 비디오 삭제 실패")


def test_delete_all_wildfire_videos():
    """특정 ID의 모든 비디오 삭제 테스트"""
    print("\n=== 테스트 12: 모든 Wildfire Videos 삭제 ===")

    service = get_wildfire_service()

    # 삭제 전 조회
    before = service.get_wildfire_videos(frfr_info_id="VIDEO_TEST_001")
    print(f"삭제 전 비디오 수: {len(before)}")

    # 모든 비디오 삭제
    success = service.delete_wildfire_videos_by_id(frfr_info_id="VIDEO_TEST_001")

    if success:
        print(f"✅ 모든 비디오 삭제 완료")
        after = service.get_wildfire_videos(frfr_info_id="VIDEO_TEST_001")
        print(f"삭제 후 비디오 수: {len(after)}")
    else:
        print(f"❌ 비디오 삭제 실패")


if __name__ == "__main__":
    print("=" * 60)
    print("TinyDB 백엔드 테스트 시작")
    print("=" * 60)

    try:
        test_create_and_retrieve()
        test_update()
        test_add_video()
        test_list_all()
        test_remove_video()
        test_delete()

        # Wildfire Video 테스트
        test_create_wildfire_video()
        test_get_wildfire_video()
        test_get_wildfire_videos_by_id()
        test_update_wildfire_video()
        test_delete_wildfire_video()
        test_delete_all_wildfire_videos()

        print("\n" + "=" * 60)
        print("✅ 모든 테스트 완료!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {e}")
        import traceback

        traceback.print_exc()