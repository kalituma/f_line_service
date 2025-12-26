"""
비디오 경로 조회 함수 사용 예제

WildfireVideoService의 새로운 함수들을 사용하여
비디오 경로 정보를 효율적으로 조회하는 방법을 보여줍니다.
"""

import sys
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from server.backend.service.video_service import WildfireVideoService


def example_1_get_video_paths_by_frfr_id():
    """예제 1: frfr_info_id로 비디오 경로 조회"""
    print("\n" + "=" * 80)
    print("예제 1: frfr_info_id로 비디오 경로 조회".center(80))
    print("=" * 80)
    
    service = WildfireVideoService()
    
    frfr_info_id = "123456"
    paths = service.get_video_paths_by_frfr_id(frfr_info_id)
    
    print(f"\n🔍 frfr_info_id: {frfr_info_id}")
    print(f"📊 총 {len(paths)}개의 비디오 경로\n")
    
    for idx, path_info in enumerate(paths, 1):
        print(f"[{idx}] 비디오명: {path_info['video_name']}")
        print(f"    경로: {path_info['video_path']}\n")


def example_2_get_video_paths_by_type():
    """예제 2: video_type으로 필터링된 경로 조회"""
    print("\n" + "=" * 80)
    print("예제 2: video_type으로 필터링된 경로 조회".center(80))
    print("=" * 80)
    
    service = WildfireVideoService()
    
    frfr_info_id = "123456"
    video_type = "FPA630"
    paths = service.get_video_paths_by_type(frfr_info_id, video_type)
    
    print(f"\n🔍 frfr_info_id: {frfr_info_id}, video_type: {video_type}")
    print(f"📊 총 {len(paths)}개의 비디오 경로\n")
    
    for idx, path_info in enumerate(paths, 1):
        print(f"[{idx}] {path_info['video_name']}")
        print(f"    {path_info['video_path']}\n")


def example_3_get_all_video_paths():
    """예제 3: 모든 비디오 경로 조회"""
    print("\n" + "=" * 80)
    print("예제 3: 모든 비디오 경로 조회".center(80))
    print("=" * 80)
    
    service = WildfireVideoService()
    
    all_paths = service.get_all_video_paths()
    
    print(f"\n📊 전체 데이터베이스의 {len(all_paths)}개 비디오 경로\n")
    
    for idx, path_info in enumerate(all_paths, 1):
        print(f"[{idx}] {path_info['video_name']}")
        print(f"    {path_info['video_path']}\n")


def example_4_dict_format():
    """예제 4: 반환 형식 확인"""
    print("\n" + "=" * 80)
    print("예제 4: 반환되는 딕셔너리 형식".center(80))
    print("=" * 80)
    
    service = WildfireVideoService()
    
    frfr_info_id = "123456"
    paths = service.get_video_paths_by_frfr_id(frfr_info_id)
    
    print(f"\n📋 반환 형식 (List[Dict[str, str]]):\n")
    print("paths = [")
    for path_info in paths[:2]:  # 처음 2개만 출력
        print(f'    {{"video_name": "{path_info["video_name"]}", "video_path": "{path_info["video_path"]}"}},')
    if len(paths) > 2:
        print(f"    ... ({len(paths) - 2}개 더)")
    print("]")
    
    print(f"\n💾 각 항목의 구조:")
    print(f"  • video_name (str): 비디오 파일명")
    print(f"  • video_path (str): 비디오 파일 경로")


def example_5_practical_use_case():
    """예제 5: 실제 사용 사례"""

    print("\n" + "=" * 80)
    print("예제 5: 실제 사용 사례 - 비디오 처리".center(80))
    print("=" * 80)
    
    service = WildfireVideoService()
    
    frfr_info_id = "186525"
    video_type = "FPA630"
    
    # FPA630 타입의 비디오 경로 조회
    paths = service.get_video_paths_by_type(frfr_info_id, video_type)
    
    print(f"\n🎬 비디오 처리 파이프라인\n")
    print(f"산불 ID: {frfr_info_id}")
    print(f"타입: {video_type}")
    print(f"개수: {len(paths)}\n")
    
    for path_info in paths:
        video_name = path_info["video_name"]
        video_path = path_info["video_path"]
        
        print(f"처리 중: {video_name}")
        print(f"  ├─ 파일 확인: {video_path}")
        print(f"  ├─ 비디오 로드")
        print(f"  ├─ 프레임 추출")
        print(f"  ├─ 분석 수행")
        print(f"  └─ 결과 저장")
        print()


def example_6_programmatic_integration():
    """예제 6: 프로그래매틱 통합"""
    print("\n" + "=" * 80)
    print("예제 6: 프로그래매틱 통합 - 외부 시스템과 연동".center(80))
    print("=" * 80)
    
    service = WildfireVideoService()
    
    print(f"\n📌 사용 사례: 비디오 처리 큐에 추가\n")
    
    # frfr_info_id별로 모든 경로 조회
    frfr_info_id = "123456"
    paths = service.get_video_paths_by_frfr_id(frfr_info_id)
    
    # 외부 시스템에 전달할 데이터 구성
    processing_queue = []
    for path_info in paths:
        task = {
            "task_id": f"task_{frfr_info_id}_{path_info['video_name']}",
            "video_name": path_info["video_name"],
            "video_path": path_info["video_path"],
            "status": "pending",
            "priority": "high"
        }
        processing_queue.append(task)
    
    print("처리 큐에 추가된 작업:")
    for task in processing_queue:
        print(f"  • task_id: {task['task_id']}")
        print(f"    video: {task['video_name']}")
        print(f"    path: {task['video_path']}")
        print()


def main():
    """메인 함수"""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  비디오 경로 조회 함수 - 사용 예제".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")
    
    try:
        example_1_get_video_paths_by_frfr_id()
        example_2_get_video_paths_by_type()
        example_3_get_all_video_paths()
        example_4_dict_format()
        example_5_practical_use_case()
        example_6_programmatic_integration()
        
        print("\n" + "=" * 80)
        print("✨ 모든 예제 완료".center(80))
        print("=" * 80)
        print("\n🎉 비디오 경로 조회 함수 사용 방법을 이해했습니다!\n")
        
    except Exception as e:
        print(f"\n❌ 에러 발생: {e}\n")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

