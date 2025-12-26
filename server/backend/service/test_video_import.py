"""
비디오 설정 파일 import 테스트 스크립트

사용 방법:
    python -m server.backend.service.test_video_import
"""

import logging
import sys
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from server.backend.service.video_service import WildfireVideoService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """메인 테스트 함수"""
    
    print("=" * 80)
    print("Wildfire Video Config Import Test")
    print("=" * 80)
    
    # 서비스 초기화
    service = WildfireVideoService()
    
    # 설정 파일 경로
    config_file_path = "config/video_config.1.json"
    
    print(f"\n📁 Config file: {config_file_path}")
    print("-" * 80)
    
    # 설정 파일에서 import
    result = service.import_from_config_file(config_file_path)
    
    # 결과 출력
    print("\n📊 Import Result:")
    print(f"  Success: {result['success']}")
    print(f"  Total: {result['total']}")
    print(f"  Imported: {result['imported']}")
    print(f"  Failed: {result['failed']}")
    
    if result['errors']:
        print(f"\n❌ Errors:")
        for error in result['errors']:
            print(f"  - {error}")
    
    # import 성공한 경우, 조회 테스트
    if result['success']:
        print("\n" + "=" * 80)
        print("📋 Querying Imported Videos")
        print("=" * 80)
        
        # config 파일에서 frfr_info_id 읽기
        import json
        with open(config_file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        frfr_info_id = config['frfr_info_id']
        
        # 저장된 비디오 조회
        videos = service.get_videos_by_frfr_id(frfr_info_id)
        
        print(f"\n✅ Retrieved {len(videos)} videos for frfr_info_id: {frfr_info_id}")
        print("-" * 80)
        
        for idx, video in enumerate(videos, 1):
            print(f"\n[{idx}] Video Info:")
            print(f"    frfr_info_id: {video.frfr_info_id}")
            print(f"    video_name: {video.video_name}")
            print(f"    video_type: {video.video_type}")
            print(f"    video_path: {video.video_path}")
        
        # video_type별 조회 테스트
        print("\n" + "=" * 80)
        print("🔍 Filtering by Video Type")
        print("=" * 80)
        
        video_types = set(video.video_type for video in videos)
        for video_type in video_types:
            filtered = service.get_videos_by_type(frfr_info_id, video_type)
            print(f"\n✅ Video Type '{video_type}': {len(filtered)} video(s)")
            for video in filtered:
                print(f"    - {video.video_name}")
    
    print("\n" + "=" * 80)
    print("✨ Test Completed")
    print("=" * 80)


if __name__ == "__main__":
    main()

