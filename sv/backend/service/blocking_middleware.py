"""
요청 블로킹 미들웨어

서버 초기화가 완료될 때까지 모든 요청을 블로킹합니다.
"""

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from sv.backend.service.app_state import get_app_state_manager, AppState
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)


class InitializationBlockerMiddleware(BaseHTTPMiddleware):
    """
    서버 초기화 완료 전 모든 요청을 블로킹하는 미들웨어
    
    초기화가 완료되면 요청이 통과됩니다.
    """
    
    async def dispatch(self, request: Request, call_next):
        """
        요청을 가로채고 초기화 상태를 확인합니다.
        
        Args:
            request: HTTP 요청
            call_next: 다음 미들웨어/핸들러
            
        Returns:
            HTTP 응답 또는 503 Service Unavailable
        """
        app_state = get_app_state_manager()
        
        # Health check는 항상 통과 (초기화 상태 확인용)
        if request.url.path == "/health":
            return await call_next(request)
        
        # 초기화 중이면 요청 블로킹
        if app_state.get_state() != AppState.READY:
            logger.warning(
                f"Request blocked during initialization: {request.method} {request.url.path}"
            )
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Server is initializing. Please try again later.",
                    "state": app_state.get_state().value
                }
            )
        
        # 초기화 완료되면 요청 처리
        response = await call_next(request)
        return response

