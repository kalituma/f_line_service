"""
Ray를 사용하여 비디오 데이터를 업데이트하는 실행 스크립트

사용 방법:
    python app/run_ray_video_daemon.py --frfr-id 186525 --analysis-id 20251222_1706_VIDEO_003
    python app/run_ray_video_daemon.py --frfr-id 186525 --analysis-id 20251222_1706_VIDEO_003 --verbose
    python app/run_ray_video_daemon.py --frfr-id 186525 --analysis-id 20251222_1706_VIDEO_003 --ray-address ray://127.0.0.1:10001
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import ray

from ray_daemon_video_update import VideoDaemonActor

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """로깅 설정"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('run_ray_video_daemon.log', encoding='utf-8')
        ]
    )


def parse_arguments() -> argparse.Namespace:
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='Ray를 사용하여 비디오 데이터를 업데이트합니다.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  # 자동 모드: DB에서 최근 데이터 자동 조회 후 한 번만 실행
  python app/run_ray_video_daemon.py
  
  # 명시적 ID 지정: 한 번만 실행
  python app/run_ray_video_daemon.py --frfr-id 186525 --analysis-id 20251222_1706_VIDEO_003
  
  # 자동 모드 + 상세 로그
  python app/run_ray_video_daemon.py --verbose
  
  # 자동 모드 + Ray 클러스터 지정
  python app/run_ray_video_daemon.py --ray-address ray://127.0.0.1:10001
  
  # 자동 모드 + Daemon: 60초마다 반복 실행 (무한)
  python app/run_ray_video_daemon.py --interval 60
  
  # 명시적 ID + Daemon: 30초마다 5번 실행
  python app/run_ray_video_daemon.py --frfr-id 186525 --analysis-id 20251222_1706_VIDEO_003 --interval 30 --max-iterations 5
  
  # 자동 모드 + 디버그 + Daemon
  python app/run_ray_video_daemon.py --debug --interval 60

변환 규칙:
  - frfr_info_id: 정수에 1을 더함 (186525 -> 186526)
  - analysis_id: minute 부분에 1을 더함 (20251222_1706_VIDEO_003 -> 20251222_1707_VIDEO_003)

    """
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='상세 로그 출력 (DEBUG 레벨)'
    )
    
    parser.add_argument(
        '--ray-address',
        type=str,
        default=None,
        help='Ray cluster 주소 (기본값: localhost:10001)'
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=0,
        help='실행 간격(초) - 0이면 한 번만 실행, > 0이면 daemon 모드로 주기 실행 (기본값: 0)'
    )
    
    parser.add_argument(
        '--max-iterations',
        type=int,
        default=-1,
        help='최대 반복 횟수 (-1이면 무한 반복, 기본값: -1)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Ray Actor 디버그 모드 활성화 (상세 로그 출력)'
    )
    
    return parser.parse_args()


def print_result(result: dict) -> None:
    """결과 출력"""
    print("\n" + "=" * 80)
    print("📊 Ray 비디오 데이터 업데이트 결과".center(80))
    print("=" * 80)
    
    print(f"\n✅ 성공 여부: {'성공' if result.get('success') else '실패'}")
    print(f"📦 처리된 비디오: {result.get('count', 0)}개")
    
    if result.get('failed_count', 0) > 0:
        print(f"❌ 실패한 비디오: {result.get('failed_count', 0)}개")
    
    print(f"\n📋 원본 데이터:")
    print(f"   frfr_info_id: {result.get('original_frfr_info_id', 'N/A')}")
    print(f"   analysis_id: {result.get('original_analysis_id', 'N/A')}")
    
    print(f"\n📋 새로운 데이터:")
    print(f"   frfr_info_id: {result.get('new_frfr_info_id', 'N/A')}")
    print(f"   analysis_id: {result.get('new_analysis_id', 'N/A')}")
    
    print(f"\n💬 메시지: {result.get('message', 'N/A')}")
    
    if result.get('errors'):
        print(f"\n⚠️ 에러 메시지:")
        for idx, error in enumerate(result['errors'], 1):
            print(f"   [{idx}] {error}")
    
    print("\n" + "=" * 80 + "\n")


def validate_analysis_id_format(analysis_id: str) -> bool:
    """analysis_id 형식 검증"""
    parts = analysis_id.split('_')
    
    if len(parts) < 3:
        print(f"❌ 에러: analysis_id 형식이 올바르지 않습니다.")
        print(f"   입력값: {analysis_id}")
        print(f"   형식: yyyyMMdd_hhmm_VIDEO_xxx")
        return False
    
    date_part = parts[0]
    time_part = parts[1]
    
    # 날짜 검증 (yyyyMMdd)
    if len(date_part) != 8 or not date_part.isdigit():
        print(f"❌ 에러: 날짜 형식이 올바르지 않습니다. (yyyyMMdd)")
        return False
    
    # 시간 검증 (hhmm)
    if len(time_part) != 4 or not time_part.isdigit():
        print(f"❌ 에러: 시간 형식이 올바르지 않습니다. (hhmm)")
        return False
    
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

    try:
        # Ray 초기화
        logger.info("Ray 초기화 중...")
        if args.ray_address:
            logger.info(f"Ray cluster 주소: {args.ray_address}")
            ray.init(address=args.ray_address, ignore_reinit_error=True)
        else:
            logger.info("Local Ray 클러스터 초기화")
            ray.init(ignore_reinit_error=True)
        
        logger.info(f"Ray 초기화 완료")
        ray_context = ray.get_runtime_context()
        logger.info(f"Ray 노드 ID: {ray_context.node_id}")
        
        # VideoDaemonActor 생성
        logger.info("VideoDaemonActor 생성 중...")
        actor = VideoDaemonActor.remote(debug=args.debug)
        logger.info(f"VideoDaemonActor 생성 완료 (Debug Mode: {args.debug})")
        
        # ID 자동 조회 (지정하지 않은 경우)
        auto_mode = False

        logger.info("자동 모드: DB에서 최근 분석 데이터 조회 중...")
        print(f"\n🔍 자동 모드: DB에서 최근 분석 데이터 조회 중...")

        # 원격 함수로 최근 ID 조회
        latest_ids_ref = actor.get_latest_analysis_ids.remote()
        latest_ids = ray.get(latest_ids_ref)

        if not latest_ids.get("success"):
            logger.error(f"최근 분석 데이터 조회 실패: {latest_ids.get('message')}")
            print(f"\n❌ {latest_ids.get('message')}")
            return 1

        frfr_id = latest_ids.get("frfr_info_id")
        analysis_id = latest_ids.get("analysis_id")
        auto_mode = True

        logger.info(f"✓ 최근 분석 데이터 자동 조회 완료")
        logger.info(f"  frfr_info_id: {frfr_id}")
        logger.info(f"  analysis_id: {analysis_id}")
        logger.info(f"  비디오명: {latest_ids.get('latest_video_name')}")
        logger.info(f"  추가 시간: {latest_ids.get('latest_add_time')}")

        print(f"✓ 조회 완료:")
        print(f"   frfr_info_id: {frfr_id}")
        print(f"   analysis_id: {analysis_id}")
        print(f"   비디오명: {latest_ids.get('latest_video_name')}")
        print(f"   추가 시간: {latest_ids.get('latest_add_time')}")
        
        # 비디오 데이터 처리
        logger.info(
            f"비디오 데이터 처리 시작: "
            f"frfr_info_id={frfr_id}, analysis_id={analysis_id}"
        )
        
        print(f"\n🚀 Ray 비디오 데이터 업데이트 시작")
        print(f"   모드: {'자동 모드' if auto_mode else '명시적 모드'}")
        print(f"   원본 frfr_info_id: {frfr_id}")
        print(f"   원본 analysis_id: {analysis_id}")
        
        # Daemon 모드 확인
        if args.interval > 0:
            print(f"   Daemon 모드: 매 {args.interval}초마다 실행")
            if args.max_iterations > 0:
                print(f"   최대 반복 횟수: {args.max_iterations}회\n")
            else:
                print(f"   최대 반복 횟수: 무한\n")
        else:
            print(f"   일회성 모드: 한 번만 실행\n")
        
        # 반복 실행
        iteration = 0
        last_frfr_id = frfr_id
        last_analysis_id = analysis_id
        
        while True:
            iteration += 1
            
            # 최대 반복 횟수 체크
            if args.max_iterations > 0 and iteration > args.max_iterations:
                logger.info(f"최대 반복 횟수({args.max_iterations})에 도달하여 종료합니다")
                break
            
            # 원격 함수 실행
            logger.info(f"[반복 {iteration}] 작업 시작")
            result_ref = actor.process_videos.remote(
                last_frfr_id,
                last_analysis_id
            )
            result = ray.get(result_ref)
            
            # 결과 출력
            print_result(result)
            
            # 다음 반복을 위해 새로운 ID 저장
            if result.get("success"):
                last_frfr_id = str(result.get("new_frfr_info_id", last_frfr_id))
                last_analysis_id = result.get("new_analysis_id", last_analysis_id)
                logger.info(f"다음 반복을 위해 ID 업데이트: frfr_info_id={last_frfr_id}, analysis_id={last_analysis_id}")
            
            # 일회성 모드면 종료
            if args.interval <= 0:
                logger.info("✅ 작업 완료 (일회성 모드)")
                return 0 if result.get("success") else 1
            
            # Daemon 모드: 대기
            logger.info(f"다음 실행까지 {args.interval}초 대기...")
            try:
                time.sleep(args.interval)
            except KeyboardInterrupt:
                logger.info("사용자에 의해 중단됨")
                print("\n⏹️  Daemon 실행이 중단되었습니다.\n")
                break
        
        logger.info("✅ Daemon 종료")
        return 0
    
    except Exception as e:
        logger.exception(f"❌ 예상치 못한 에러 발생: {e}")
        print(f"\n❌ 예상치 못한 에러가 발생했습니다:")
        print(f"   {str(e)}\n")
        return 1
    
    finally:
        if ray.is_initialized():
            logger.info("Ray 종료 중...")
            ray.shutdown()
            logger.info("Ray 종료 완료")


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

