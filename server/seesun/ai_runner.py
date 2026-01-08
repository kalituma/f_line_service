"""
AI 분석 시작 함수를 연결하기 위한 어댑터.
실제 AI 모듈의 분석 시작 로직으로 교체해 사용한다.
"""

from loguru import logger


def start_analysis(event_cd: str, analysis_cd: str):
    """
    AI 분석을 시작한다.
    필요한 경우 이 함수를 실제 AI 진입점으로 대체하거나 진입점 관련 함수로 교체
    """
    # TODO: 아래 로직을 실제 AI 분석 시작 코드로 교체하세요.
    logger.info("AI 분석 시작 요청: event_cd={} analysis_cd={}", event_cd, analysis_cd)

