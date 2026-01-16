from typing import Dict, Any, Tuple
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger
from sv.daemon.module.http_request_client import post_json, HttpRequestError

logger = setup_logger(__name__)

class ConnectionTask(TaskBase):
    def __init__(
        self, 
        api_url: str,
        should_fail: bool = False,
        fail_message: str = "의도적인 테스트 에러",
        delay_seconds: float = None,
        raise_exception: Exception = None
    ):
        """
        Args:
            api_url: API 엔드포인트 URL
            should_fail: True이면 실행 시 에러 발생
            fail_message: 에러 발생 시 사용할 메시지
            delay_seconds: 작업 실행 시 지연 시간(초). None이면 지연 없음
            raise_exception: 작업 실행 시 발생시킬 예외. None이면 정상 실행
        """
        super().__init__("ConnectionTask", delay_seconds, raise_exception)
        self.api_url = api_url
        self.should_fail = should_fail
        self.fail_message = fail_message

    def get_ids(self, context: Dict[str, Any]) -> Tuple[str, str]:
        loop_context = context.get('loop_context', {})
        if not loop_context:
            logger.error("❌ loop_context이 context에 없습니다")
            return
        frfr_id = loop_context.get('frfr_id')
        analysis_id = loop_context.get('analysis_id')
        return frfr_id, analysis_id

    def before_execute(self, context: Dict[str, Any]) -> None:
        frfr_id, analysis_id = self.get_ids(context)

        """작업 실행 전 준비 작업"""
        logger.info(
            f"🔄 ConnectionTask 시작 준비 - frfr_id={frfr_id}, analysis_id={analysis_id}")

    def _execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """실제 작업 실행 - HTTP 요청으로 데이터 수집"""
        logger.info("▶️  ConnectionTask 실행 중...")
        frfr_id, analysis_id = self.get_ids(context)

        # 🔥 테스트용: 의도적 에러 발생
        if self.should_fail:
            logger.error(f"❌ 의도적 에러 발생: {self.fail_message}")
            raise RuntimeError(self.fail_message)

        # 요청 데이터 구성
        request_data = {
            "frfr_info_id": frfr_id,
            "analysis_id": analysis_id
        }
        
        logger.info(f"📤 API 요청 시작: POST {self.api_url}")
        logger.info(f"📋 요청 데이터: {request_data}")
        
        try:
            # POST 요청으로 데이터 수집
            result = post_json(
                url=self.api_url,
                json_data=request_data,
                timeout=30,
                verify_ssl=False
            )
            
            logger.info(f"📥 API 응답 수신 완료: {result}")
            return result
            
        except HttpRequestError as e:
            logger.error(f"❌ API 요청 실패: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"❌ 예상치 못한 에러 발생: {str(e)}", exc_info=True)
            raise

    def after_execute(self, context: Dict[str, Any], result: Dict[str, Any]) -> None:
        """작업 실행 후 정리 작업"""
        logger.info("✅ ConnectionTask 완료 - 응답 데이터 수신 완료")
        logger.info(f"📊 응답 데이터: {result}")

    def on_error(self, context: Dict[str, Any], error: Exception) -> None:
        """에러 발생 시 처리"""
        logger.error(f"❌ ConnectionTask 에러 발생: {str(error)}", exc_info=True)
