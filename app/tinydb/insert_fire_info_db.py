"""
사용 방법:
    python -m app.insert_fire_info_db config/fire_loc_config.1.json
    python app/insert_fire_info_db.py config/fire_loc_config.1.json --verbose
"""

from typing import Optional, Dict, Any
import json
import os
import sys
import argparse
import logging
from pathlib import Path

from server.backend.service.fire_info_service import FireInfoService
from server.utils.config_util import validate_config, load_config_file

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """로깅 설정"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('insert_fire_info_db.log', encoding='utf-8')
        ]
    )


def print_result(result: dict, config_file_path: str) -> None:
    """결과 출력"""
    print("\n" + "-" * 80)
    print("📊 Import 결과".center(80))
    print("-" * 80)
    
    print(f"\n📁 설정 파일: {config_file_path}")
    print(f"✅ 성공 여부: {'성공' if result['success'] else '실패'}")
    
    if result['success']:
        print(f"📝 Record ID: {result['record_id']}")
        print(f"🔥 산불 ID (frfr_info_id): {result['frfr_info_id']}")
        print(f"📍 위도: {result['fire_location']['latitude']}")
        print(f"📍 경도: {result['fire_location']['longitude']}")
    else:
        print(f"❌ 에러: {result.get('error', '알 수 없는 에러')}")
    
    print("\n" + "=" * 80 + "\n")


def print_fire_location_info(service: FireInfoService, frfr_info_id: str) -> None:
    """저장된 산불 위치 정보 출력"""
    fire_location = service.get_fire_location(frfr_info_id)
    
    if not fire_location:
        print(f"⚠️ {frfr_info_id}에 대한 산불 위치 정보가 없습니다.\n")
        return
    
    print(f"\n📋 저장된 산불 위치 정보 (frfr_info_id: {frfr_info_id})")
    print("-" * 80)
    print(f"  📍 위도: {fire_location.get('fire_location', {}).get('latitude', 'N/A')}")
    print(f"  📍 경도: {fire_location.get('fire_location', {}).get('longitude', 'N/A')}")
    print()


def parse_arguments() -> argparse.Namespace:
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='산불 위치 설정 파일을 읽어서 DB에 저장합니다.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  python -m app.insert_fire_info_db config/fire_loc_config.1.json
  python app/insert_fire_info_db.py config/fire_loc_config.1.json --verbose
        """
    )
    
    parser.add_argument(
        'config_file',
        type=str,
        help='JSON 설정 파일 경로 (필수)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='상세 로그 출력 (DEBUG 레벨)'
    )

    return parser.parse_args()


def validate_config_file(config_file_path: str) -> bool:
    """설정 파일 존재 여부 확인"""
    path = Path(config_file_path)
    
    if not path.exists():
        print(f"❌ 에러: 파일을 찾을 수 없습니다: {config_file_path}")
        return False
    
    if not path.is_file():
        print(f"❌ 에러: {config_file_path}는 파일이 아닙니다.")
        return False
    
    if path.suffix.lower() != '.json':
        print(f"⚠️ 경고: {config_file_path}는 JSON 파일이 아닐 수 있습니다.")
    
    return True


def load_config(config_file_path: str) -> Optional[Dict[str, Any]]:
    """설정 파일 로드 및 검증"""
    required_fields = ["frfr_info_id", "location"]

    # 1. 설정 파일 로드
    config_data = load_config_file(config_file_path)
    if config_data is None:        
        raise Exception("설정 파일 로드 실패")

    # 2. 설정 검증
    if not validate_config(config_data, required_fields):
        raise Exception("설정 파일 검증 실패")

    return config_data


def main() -> int:
    """
    메인 함수
    
    Returns:
        종료 코드 (0: 성공, 1: 실패)
    """
    # 명령줄 인자 파싱
    args = parse_arguments()
    
    # 로깅 설정
    setup_logging(args.verbose)
    
    # 설정 파일 검증
    if not validate_config_file(args.config_file):
        return 1
    
    try:
        # FireInfoService 초기화
        logger.info("FireInfoService 초기화 중...")
        service = FireInfoService()
        
        # 설정 파일 로드
        logger.info(f"Import 시작: {args.config_file}")
        fire_config = load_config(args.config_file)
        
        # 산불 위치 정보 저장
        result = service.save_fire_location(fire_config)
        
        if result['success']:
            logger.info("✅ Import 완료 (성공)")
            print("✅ 산불 위치 정보가 성공적으로 저장되었습니다.")
            print_result(result, args.config_file)
            
            # 저장된 정보 확인
            print_fire_location_info(service, fire_config['frfr_info_id'])
            return 0
        else:
            logger.error(f"❌ Import 실패: {result.get('error', '알 수 없는 에러')}")
            print(f"\n❌ 산불 위치 정보 저장에 실패했습니다:")
            print(f"   {result.get('error', '알 수 없는 에러')}\n")
            return 1
    
    except Exception as e:
        logger.exception(f"❌ 예상치 못한 에러 발생: {e}")
        print(f"\n❌ 예상치 못한 에러가 발생했습니다:")
        print(f"   {str(e)}\n")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

