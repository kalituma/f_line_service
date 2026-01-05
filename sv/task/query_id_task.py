import logging
import requests
from typing import Dict, Any
from sv.task.task_base import TaskBase

logger = logging.getLogger(__name__)


class QueryIdTask(TaskBase):
    """URL에서 JSON 데이터를 요청하고 처리하는 Task"""

    def __init__(self, task_name: str = "QueryIdTask"):
        """
        QueryIdTask 초기화
        
        Args:
            task_name: 작업 이름 (기본값: "QueryIdTask")
        """
        super().__init__(task_name)

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        초기 argument로 전달받은 URL에서 JSON을 요청하고 출력
        
        Args:
            context: 다음 키를 포함해야 함:
                - url (str): 요청할 URL
        
        Returns:
            실행 결과 딕셔너리:
                {
                    "success": bool,
                    "data": dict (요청 결과 JSON),
                    "error": str (에러 메시지, 실패 시에만 포함)
                }
        """
        try:
            # context에서 url 추출
            url = context.get("url")
            
            if not url:
                self.logger.error("URL이 context에 포함되어 있지 않습니다")
                return {
                    "success": False,
                    "error": "URL이 context에 포함되어 있지 않습니다"
                }
            
            self.logger.info(f"URL에 요청 시작: {url}")
            
            # HTTP GET 요청
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # 상태 코드 확인
            
            # JSON 파싱
            data = response.json()
            
            # 결과 출력 (로깅)
            self.logger.info(f"요청 성공 (상태 코드: {response.status_code})")
            self.logger.info(f"응답 데이터:\n{data}")
            
            # 결과 반환
            result = {
                "success": True,
                "data": data,
                "status_code": response.status_code,
                "url": url
            }
            
            return result
        
        except requests.exceptions.Timeout:
            error_msg = f"요청 타임아웃 (URL: {context.get('url')})"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
        
        except requests.exceptions.ConnectionError:
            error_msg = f"연결 오류 (URL: {context.get('url')})"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
        
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP 오류: {e.response.status_code} (URL: {context.get('url')})"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "status_code": e.response.status_code
            }
        
        except requests.exceptions.RequestException as e:
            error_msg = f"요청 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
        
        except ValueError as e:
            error_msg = f"JSON 파싱 오류: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
        
        except Exception as e:
            error_msg = f"예상치 못한 오류: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {
                "success": False,
                "error": error_msg
            }

