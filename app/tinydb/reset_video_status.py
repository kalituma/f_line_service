"""
비디오 상태 일괄 업데이트 프로그램
사용자로부터 입력받은 frfr_id, analysis_id를 활용하여
CHECK_URL에서 조회한 모든 비디오의 상태를 STAT_001로 업데이트합니다.
"""

import sys
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List

# 프로젝트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sv.daemon.module.http_request_client import post_json, HttpRequestError
from sv.daemon.server_state import ServerAnalysisStatus
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

# 체크할 서버 URL
CHECK_URL = "http://127.0.0.1:8086/wildfire-data-sender/api/wildfire/sender"
# 업데이트할 서버 URL
UPDATE_URL = "http://127.0.0.1:8086/wildfire-data-receiver/api/wildfire/video-status"


def get_user_input() -> Dict[str, Any]:
    """
    argparse를 사용하여 명령줄 인자로부터 정보를 받습니다.
    
    Returns:
        frfr_id, analysis_id를 포함한 딕셔너리
    """
    parser = argparse.ArgumentParser(
        description="🎬 비디오 상태 일괄 업데이트 프로그램",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python reset_video_status.py --frfr-id 123456 --analysis-id 20251222_1706_VIDEO_003
  python reset_video_status.py -f 123456 -a 20251222_1706_VIDEO_003
        """
    )
    
    parser.add_argument(
        '--frfr-id', '-f',
        dest='frfr_id',
        required=True,
        help='FRFR 정보 ID (필수)'
    )
    
    parser.add_argument(
        '--analysis-id', '-a',
        dest='analysis_id',
        required=True,
        help='분석 ID (필수)'
    )
    
    parser.add_argument(
        '--status', '-s',
        dest='status',
        default='STAT_001',
        choices=['STAT_001', 'STAT_002', 'STAT_003'],
        help='업데이트할 분석 상태 (기본값: STAT_001)'
    )
    
    parser.add_argument(
        '--check-url', '-c',
        dest='check_url',
        default=CHECK_URL,
        help=f'비디오 조회 URL (기본값: {CHECK_URL})'
    )
    
    parser.add_argument(
        '--update-url', '-u',
        dest='update_url',
        default=UPDATE_URL,
        help=f'상태 업데이트 URL (기본값: {UPDATE_URL})'
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 80)
    print("🎬 비디오 상태 일괄 업데이트 프로그램")
    print("=" * 80)
    print(f"\n📍 입력 정보:")
    print(f"   frfr_id: {args.frfr_id}")
    print(f"   analysis_id: {args.analysis_id}")
    print(f"   status: {args.status}")
    print(f"   check_url: {args.check_url}")
    print(f"   update_url: {args.update_url}")
    
    return {
        'frfr_id': args.frfr_id,
        'analysis_id': args.analysis_id,
        'status': args.status,
        'check_url': args.check_url,
        'update_url': args.update_url
    }


def fetch_video_data(frfr_id: str, analysis_id: str, check_url: str) -> Dict[str, Any]:
    """
    CHECK_URL에서 비디오 정보를 조회합니다.
    
    Args:
        frfr_id: FRFR 정보 ID
        analysis_id: 분석 ID
        check_url: 비디오 조회 URL
    
    Returns:
        조회된 비디오 데이터
    
    Raises:
        HttpRequestError: 요청 실패 시
    """
    query_data = {
        "frfr_info_id": frfr_id,
        "analysis_id": analysis_id
    }
    
    logger.info("=" * 80)
    logger.info(f"📥 비디오 정보 조회 시작")
    logger.info(f"   URL: {check_url}")
    logger.info(f"   frfr_id: {frfr_id}, analysis_id: {analysis_id}")
    logger.info("=" * 80)
    
    print("\n📋 조회 요청 데이터:")
    print(json.dumps(query_data, indent=2, ensure_ascii=False))
    
    # POST 요청으로 데이터 조회
    response = post_json(
        url=check_url,
        json_data=query_data,
        timeout=30,
        verify_ssl=False
    )
    
    logger.info(f"✅ 비디오 정보 조회 성공")
    
    print("\n📋 조회 응답 데이터:")
    print(json.dumps(response, indent=2, ensure_ascii=False))
    
    return response


def extract_video_names(video_data: Dict[str, Any]) -> List[str]:
    """
    조회된 데이터에서 비디오 이름 목록을 추출합니다.
    
    Args:
        video_data: CHECK_URL에서 조회된 비디오 데이터
    
    Returns:
        비디오 이름 목록
    """
    videos = video_data.get('videos', [])
    video_names = [video.get('video_name') for video in videos if video.get('video_name')]
    return video_names


def build_update_request_data(frfr_id: str, analysis_id: str, video_names: List[str], status: str = 'STAT_001') -> Dict[str, Any]:
    """
    일괄 업데이트 요청 데이터를 구성합니다.
    
    Args:
        frfr_id: FRFR 정보 ID
        analysis_id: 분석 ID
        video_names: 업데이트할 비디오 이름 목록
        status: 분석 상태 (기본값: STAT_001)
    
    Returns:
        POST 요청에 사용할 데이터
    """
    # 상태 문자열을 ServerAnalysisStatus 열거형으로 변환
    status_enum = getattr(ServerAnalysisStatus, status)
    
    video_updates = [
        {
            "video_name": video_name,
            "analysis_status": status_enum.to_code()
        }
        for video_name in video_names
    ]
    
    request_data = {
        "frfr_info_id": frfr_id,
        "analysis_id": analysis_id,
        "video_updates": video_updates
    }
    return request_data


def update_all_video_status(frfr_id: str, analysis_id: str, status: str = 'STAT_001', 
                            check_url: str = None, update_url: str = None) -> bool:
    """
    서버에서 조회한 모든 비디오의 상태를 일괄 업데이트합니다.
    
    Args:
        frfr_id: FRFR 정보 ID
        analysis_id: 분석 ID
        status: 업데이트할 분석 상태 (기본값: STAT_001)
        check_url: 비디오 조회 URL (기본값: CHECK_URL)
        update_url: 상태 업데이트 URL (기본값: UPDATE_URL)
    
    Returns:
        성공 여부
    """
    if check_url is None:
        check_url = CHECK_URL
    if update_url is None:
        update_url = UPDATE_URL
    
    try:
        # Step 1: 비디오 정보 조회
        print("\n\n" + "=" * 80)
        print("🔍 단계 1: 비디오 정보 조회")
        print("=" * 80)
        
        video_data = fetch_video_data(frfr_id, analysis_id, check_url)
        
        # Step 2: 비디오 이름 추출
        print("\n\n" + "=" * 80)
        print("🔍 단계 2: 비디오 이름 추출")
        print("=" * 80)
        
        video_names = extract_video_names(video_data)
        
        if not video_names:
            logger.warning("⚠️  조회된 비디오가 없습니다.")
            print("\n⚠️  조회된 비디오가 없습니다.")
            return False
        
        logger.info(f"📹 추출된 비디오 ({len(video_names)}개): {video_names}")
        print(f"\n📹 추출된 비디오 ({len(video_names)}개):")
        for i, name in enumerate(video_names, 1):
            print(f"   {i}. {name}")
        
        # Step 3: 업데이트 요청 데이터 구성
        print("\n\n" + "=" * 80)
        print("🔍 단계 3: 업데이트 요청 데이터 구성")
        print("=" * 80)
        
        request_data = build_update_request_data(frfr_id, analysis_id, video_names, status)
        
        logger.info(f"📋 업데이트 요청 데이터 생성 완료")
        print("\n📋 업데이트 요청 데이터:")
        print(json.dumps(request_data, indent=2, ensure_ascii=False))
        
        # Step 4: 비디오 상태 일괄 업데이트
        print("\n\n" + "=" * 80)
        print("🔍 단계 4: 비디오 상태 일괄 업데이트")
        print("=" * 80)
        
        logger.info("=" * 80)
        logger.info(f"📤 비디오 상태 일괄 업데이트 요청 시작")
        logger.info(f"   URL: {update_url}")
        logger.info(f"   frfr_id: {frfr_id}, analysis_id: {analysis_id}")
        logger.info(f"   status: {status}")
        logger.info(f"   video_count: {len(video_names)}")
        logger.info("=" * 80)
        
        # POST 요청으로 데이터 업데이트
        response = post_json(
            url=update_url,
            json_data=request_data,
            timeout=30,
            verify_ssl=False
        )
        
        logger.info(f"✅ 데이터 일괄 업데이트 성공: {response}")
        
        print("\n" + "=" * 80)
        print("✅ 비디오 상태 일괄 업데이트 성공!")
        print("=" * 80)
        print(f"\n📋 업데이트 응답 데이터:")
        print(json.dumps(response, indent=2, ensure_ascii=False))
        print(f"\n📊 업데이트 요약:")
        print(f"   총 비디오 수: {len(video_names)}")
        print(f"   업데이트 상태: {status}")
        
        return True
        
    except HttpRequestError as e:
        logger.error(f"❌ 요청 실패: {str(e)}", exc_info=True)
        print("\n" + "=" * 80)
        print(f"❌ 요청 실패: {str(e)}")
        print("=" * 80)
        return False
        
    except Exception as e:
        logger.error(f"❌ 예상치 못한 에러: {str(e)}", exc_info=True)
        print("\n" + "=" * 80)
        print(f"❌ 예상치 못한 에러가 발생했습니다: {str(e)}")
        print("=" * 80)
        return False


def main():
    """메인 프로그램"""
    try:
        # 명령줄 인자 받기
        user_input = get_user_input()
        
        # 모든 비디오 상태 일괄 업데이트
        success = update_all_video_status(
            frfr_id=user_input['frfr_id'],
            analysis_id=user_input['analysis_id'],
            status=user_input['status'],
            check_url=user_input['check_url'],
            update_url=user_input['update_url']
        )
        
        # 종료 코드 반환
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  프로그램이 사용자에 의해 중단되었습니다.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ 프로그램 실행 중 에러: {str(e)}", exc_info=True)
        print(f"\n❌ 프로그램 실행 중 에러: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
