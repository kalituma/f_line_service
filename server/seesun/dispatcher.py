"""
AI 모델로 분석 그룹을 순차 전송하는 스케줄러의 핵심 루프.
"""
import sys
import time
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Optional, List

import requests
from loguru import logger

from server.seesun.config import settings
from server.seesun.ai_runner import start_analysis
from server.seesun.state_store import StateStore

from sv.daemon.module.http_request_client import get_json

def fetch_group001_pending() -> List[Dict]:
    """
    HTTP GET 요청으로 wildfire-data-sender API에서 데이터를 가져온다.
    응답 형식: {"total_count": 1, "data": [{"frfr_info_id": "123456", "analysis_id": "20251222_1706_VIDEO_003"}]}
    """
    try:
        url = "http://127.0.0.1:8086/wildfire-data-sender/api/wildfire/sender"
        response_data = get_json(url, timeout=10, verify_ssl=False)
        
        logger.info(f"Wildfire sender API 응답: {response_data}")
        
        mock_data = []
        if isinstance(response_data, dict) and "data" in response_data:
            for item in response_data.get("data", []):
                mock_data.append({
                    "analysis_id": item.get("analysis_id"),
                    "event_cd": item.get("frfr_info_id"),
                    "status": "GROUP_001",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })
        
        logger.info(f"변환된 mock_data: {mock_data}")
        return mock_data
    except Exception as e:
        logger.error(f"Wildfire sender API 요청 실패: {e}")
        # 실패 시 빈 리스트 반환
        return []


def fetch_statuses(sent_ids: List[str]) -> Dict[str, str]:
    """
    주어진 analysis_id 목록에 대해 가상의 상태 데이터를 반환한다.
    실제 database 대신 테스트용 mock 상태를 반환한다.
    """
    statuses = {}
    for aid in sent_ids:
        # 임의로 GROUP_002, GROUP_003, GROUP_004 중 하나를 선택
        status = random.choice(["GROUP_002", "GROUP_003", "GROUP_004"])
        statuses[aid] = status
    return statuses


def mark_as_in_progress(analysis_id: str) -> None:
    """
    주어진 analysis_id를 in_progress 상태로 표시한다.
    실제 database 대신 로깅만 수행한다.
    """
    logger.debug(f"[Mock] analysis_id={analysis_id}를 in_progress 상태로 표시")


# ===== AnalysisDispatcher 클래스 =====
class AnalysisDispatcher:
    def __init__(self, send_func: Optional[Callable[[Dict], bool]] = None, run_once: bool = False):
        """로깅, 상태 저장소를 초기화하고 전송 함수를 주입한다."""
        logger.remove()
        logger.add(sys.stdout, level=settings.log_level)
        self.state = StateStore(settings.state_file)
        self.run_once = run_once
        # 기본값은 내부 함수 호출; 필요 시 HTTP 전송 함수로 교체 가능
        self.send_func = send_func or self._send_to_ai_local
        logger.info(
            "분석 디스패처 초기화: 전송 간격={}분, 폴링주기={}초, 1회 실행 모드={}",
            settings.dispatch_interval_minutes,
            settings.poll_interval_seconds,
            self.run_once,
        )

    def run_forever(self):
        """폴링 주기에 따라 무한 루프로 디스패치를 수행한다. run_once=True면 1회만 실행한다."""
        while True:
            try:
                self._tick()
            except Exception as exc:
                logger.exception(f"디스패치 주기 실패: {exc}")
            
            if self.run_once:
                logger.info("1회 실행 모드: 디스패치 종료")
                break
            
            time.sleep(settings.poll_interval_seconds)

    def _tick(self):
        """한 번의 디스패치 사이클을 수행한다."""
        self._prune_completed_dispatches()

        if not self.run_once:
            if not self._can_dispatch_now():
                return

        candidate = self._next_candidate()
        if not candidate:
            return

        if self.send_func(candidate):
            mark_as_in_progress(candidate["analysis_id"])
            self.state.mark_sent(candidate["analysis_id"], candidate["event_cd"])
            logger.info(
                "전송 대기열 등록: analysis_id={} event_cd={} 시각={}",
                candidate["analysis_id"],
                candidate["event_cd"],
                datetime.now(timezone.utc).isoformat(),
            )
        else:
            # 실패 시 과도한 재시도를 막기 위해 대기 시간을 늘릴 수 있다.
            if settings.backoff_on_failure:
                self.state.bump_last_dispatch()

    def _can_dispatch_now(self) -> bool:
        """전송 간격이 지났는지 판단한다."""
        last_sent = self.state.last_dispatch_at()
        # 한 번도 전송하지 않았다면 즉시 전송을 허용한다.
        if not last_sent:
            return True
        elapsed = datetime.now(timezone.utc) - last_sent
        return elapsed.total_seconds() >= settings.dispatch_interval_seconds

    def _next_candidate(self) -> Optional[Dict]:
        """GROUP_001 중 아직 보내지 않은 가장 이른 항목을 선택한다."""
        rows = fetch_group001_pending()
        if not rows:
            logger.info("대기 중인 GROUP_001 행이 없음")
            return None

        logger.info("대기 GROUP_001 행 {}건 조회", len(rows))
        for row in rows:
            analysis_id = row.get("analysis_id")
            if not analysis_id:
                continue
            if self.state.is_sent(analysis_id):
                continue
            logger.info(
                "전송 후보 선택: analysis_id={} event_cd={}",
                analysis_id,
                row.get("event_cd"),
            )
            return row
        logger.info("모든 대기 GROUP_001 항목이 이미 전송됨; 완료 상태를 대기")
        return None

    def _send_to_ai_local(self, record: Dict) -> bool:
        """선택된 분석 그룹을 AI 내부 함수로 직접 전달한다."""
        try:
            start_analysis(
                event_cd=record.get("event_cd"),
                analysis_cd=record.get("analysis_id"),
            )
            return True
        except Exception as exc:
            logger.error(
                "AI 내부 분석 시작 함수 호출 실패: analysis_id={} event_cd={} error={}",
                record.get("analysis_id"),
                record.get("event_cd"),
                exc,
            )
            return False

    def _send_to_ai_http(self, record: Dict) -> bool:
        """선택된 분석 그룹을 AI HTTP 엔드포인트로 전송한다."""
        if not settings.ai_dispatch_url:
            logger.error("AI 디스패치 URL이 설정되어 있지 않습니다.")
            return False
        payload = {
            "frfr_info_id": record.get("event_cd"),
            "analysis_id": record.get("analysis_id"),
        }
        headers = {"Content-Type": "application/json"}
        if settings.ai_auth_token:
            headers["Authorization"] = f"Bearer {settings.ai_auth_token}"

        logger.info(
            "AI 엔드포인트={} 로 페이로드 전송={}",
            settings.ai_dispatch_url,
            payload,
        )

        try:
            response = requests.post(
                settings.ai_dispatch_url,
                json=payload,
                headers=headers,
                timeout=settings.request_timeout_seconds,
            )
        except requests.RequestException as exc:
            logger.error(
                "AI 디스패치 엔드포인트 요청 실패: analysis_id={} frfr_info_id={} error={}",
                record.get("analysis_id"),
                record.get("event_cd"),
                exc,
            )
            return False

        if 200 <= response.status_code < 300:
            logger.info(
                "전송 성공: analysis_id={} frfr_info_id={} status={} body={}",
                record.get("analysis_id"),
                record.get("event_cd"),
                response.status_code,
                response.text,
            )
            return True

        logger.error(
            "전송 실패: analysis_id={} frfr_info_id={} status={} response={}",
            record.get("analysis_id"),
            record.get("event_cd"),
            response.status_code,
            response.text,
        )
        return False

    def _prune_completed_dispatches(self):
        """DB 상태를 확인해 완료된 전송을 로컬 상태에서 제거한다."""
        sent_ids = list(self.state.state.get("sent", {}).keys())
        if not sent_ids:
            return

        statuses = fetch_statuses(sent_ids)
        completed = [
            aid
            for aid, status in statuses.items()
            if status in ("GROUP_003", "GROUP_004")
        ]
        removed = self.state.prune_completed(completed)
        if removed:
            logger.info("로컬 상태에서 완료된 전송 %s건 제거", removed)
