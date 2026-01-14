"""
분석 상태 설정 파일을 argument로 받아서 DB에 insert하는 CLI 애플리케이션

사용 방법:
    python -m app.insert_analysis_status_db config/analysis_status_config.1.json
    python app/insert_analysis_status_db.py config/analysis_status_config.1.json --verbose
"""

import sys
import argparse
import logging
from pathlib import Path

from server.backend.service.analysis_status_service import AnalysisStatusService


def setup_logging(verbose: bool = False) -> None:
    """로깅 설정"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('insert_analysis_status_db.log', encoding='utf-8')
        ]
    )


def print_result(result: dict, config_file_path: str) -> None:
    """결과 출력"""
    print("\n" + "-" * 80)
    print("📊 분석 상태 Import 결과".center(80))
    print("-" * 80)
    
    print(f"\n📁 설정 파일: {config_file_path}")
    print(f"✅ 성공 여부: {'성공' if result['success'] else '실패'}")
    print(f"📦 총 개수: {result['total_count']}개")
    print(f"✔️ Import 성공: {result['saved_count']}개")
    print(f"❌ 실패: {result['failed_count']}개")
    
    if result['errors']:
        print(f"\n⚠️ 에러 메시지:")
        for idx, error in enumerate(result['errors'], 1):
            print(f"   [{idx}] {error}")
    
    if result.get('record_ids'):
        print(f"\n📋 저장된 레코드 ID:")
        for idx, record_id in enumerate(result['record_ids'], 1):
            print(f"   [{idx}] {record_id}")
    
    print("\n" + "=" * 80 + "\n")


def print_imported_status(
    service: AnalysisStatusService,
    analysis_id: str,
    frfr_info_id: str
) -> None:
    """Import된 분석 상태 정보 출력"""
    status_list = service.get_analysis_status(analysis_id, frfr_info_id)
    
    if not status_list:
        print(
            f"⚠️ {analysis_id}/{frfr_info_id}에 대한 분석 상태 정보가 없습니다.\n"
        )
        return
    
    print(f"\n📋 Import된 분석 상태 정보")
    print(f"   분석 ID: {analysis_id}")
    print(f"   산불 정보 ID: {frfr_info_id}")
    print("-" * 80)
    print(f"총 {len(status_list)}개의 분석 상태 레코드\n")
    
    if isinstance(status_list, dict):
        # 단일 레코드인 경우
        print(f"  📺 {status_list.get('video_name', 'N/A')}")
        print(f"     상태: {status_list.get('analysis_status', 'N/A')}")
    else:
        # 리스트인 경우
        for idx, status in enumerate(status_list, 1):
            print(f"  [{idx}] {status.get('video_name', 'N/A')}")
            print(f"       상태: {status.get('analysis_status', 'N/A')}")
    
    print()


def parse_arguments() -> argparse.Namespace:
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='분석 상태 설정 파일을 읽어서 DB에 저장합니다.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  python -m app.insert_analysis_status_db config/analysis_status_config.1.json
  python app/insert_analysis_status_db.py config/analysis_status_config.1.json --verbose
  python app/insert_analysis_status_db.py config/analysis_status_config.1.json --show-status
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
    
    parser.add_argument(
        '-s', '--show-status',
        action='store_true',
        help='Import 완료 후 저장된 분석 상태 정보 표시'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='설정 파일 검증만 하고 실제 import는 하지 않음 (구현 예정)'
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
    logger = logging.getLogger(__name__)
    
    # 설정 파일 검증
    if not validate_config_file(args.config_file):
        return 1
    
    try:
        # AnalysisStatusService 초기화
        logger.info("AnalysisStatusService 초기화 중...")
        service = AnalysisStatusService()
        
        # JSON 파일 읽기
        import json
        logger.info(f"설정 파일 읽기: {args.config_file}")
        with open(args.config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        # Import 실행
        logger.info(f"Import 시작: {args.config_file}")
        result = service.save_analysis_status_batch(config_data)
        
        # 결과 출력
        print_result(result, args.config_file)
        
        # 실패한 경우 에러 코드 반환
        if not result['success']:
            logger.error("Import 실패")
            return 1
        
        # --show-status 옵션 처리
        if args.show_status and result['success']:
            analysis_id = config_data.get('analysis_id')
            frfr_info_id = config_data.get('frfr_info_id')
            if analysis_id and frfr_info_id:
                print_imported_status(service, analysis_id, frfr_info_id)
        
        logger.info("✅ Import 완료 (성공)")
        print("✅ 모든 작업이 성공적으로 완료되었습니다.\n")
        return 0
    
    except Exception as e:
        logger.exception(f"❌ 예상치 못한 에러 발생: {e}")
        print(f"\n❌ 예상치 못한 에러가 발생했습니다:")
        print(f"   {str(e)}\n")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

