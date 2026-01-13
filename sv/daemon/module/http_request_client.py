"""
HTTP Request Client Library

GET 및 POST 방식으로 HTTP 요청을 보낼 수 있는 라이브러리입니다.
Parameter, Headers, Timeout 등을 유연하게 설정할 수 있습니다.
"""

import requests
import json
from typing import Dict, Any, Optional, Union
from urllib.parse import urljoin
import logging

logger = logging.getLogger(__name__)


class HttpRequestError(Exception):
    """HTTP 요청 실패 시 발생하는 커스텀 예외"""
    pass


def get_request(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
    verify_ssl: bool = True
) -> requests.Response:
    """
    GET 방식으로 HTTP 요청을 보냅니다.
    
    Args:
        url: 요청할 URL (예: "http://example.com/api/data")
        params: URL 쿼리 파라미터 딕셔너리 (예: {"key": "value", "id": 123})
        headers: 요청 헤더 딕셔너리 (예: {"Authorization": "Bearer token"})
        timeout: 요청 타임아웃 시간 (초 단위, 기본값 10초)
        verify_ssl: SSL 인증서 검증 여부 (기본값 True)
    
    Returns:
        requests.Response: 응답 객체
    
    Raises:
        HttpRequestError: 요청 실패 시
    
    Examples:
        >>> response = get_request("http://example.com/api/users")
        >>> response.json()
        
        >>> response = get_request(
        ...     "http://example.com/api/users",
        ...     params={"page": 1, "limit": 10}
        ... )
        
        >>> response = get_request(
        ...     "http://example.com/api/users",
        ...     params={"user_id": 123},
        ...     headers={"Authorization": "Bearer token123"}
        ... )
    """
    try:
        logger.info(f"GET 요청 시작: {url}, params={params}")
        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            verify=verify_ssl
        )
        response.raise_for_status()
        logger.info(f"GET 요청 성공: {url}, status_code={response.status_code}")
        return response
    except requests.exceptions.Timeout:
        error_msg = f"GET 요청 타임아웃: {url} (timeout={timeout}초)"
        logger.error(error_msg)
        raise HttpRequestError(error_msg)
    except requests.exceptions.ConnectionError as e:
        error_msg = f"GET 요청 연결 실패: {url}, error={str(e)}"
        logger.error(error_msg)
        raise HttpRequestError(error_msg)
    except requests.exceptions.HTTPError as e:
        error_msg = f"GET 요청 HTTP 오류: {url}, status_code={response.status_code}, error={str(e)}"
        logger.error(error_msg)
        raise HttpRequestError(error_msg)
    except Exception as e:
        error_msg = f"GET 요청 실패: {url}, error={str(e)}"
        logger.error(error_msg)
        raise HttpRequestError(error_msg)


def post_request(
    url: str,
    data: Optional[Union[Dict[str, Any], str]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
    verify_ssl: bool = True
) -> requests.Response:
    """
    POST 방식으로 HTTP 요청을 보냅니다.
    
    Args:
        url: 요청할 URL (예: "http://example.com/api/users")
        data: 폼 데이터 형식의 요청 본문 (예: {"username": "john", "email": "john@example.com"})
        json_data: JSON 형식의 요청 본문 (예: {"name": "John", "age": 30})
                  data와 json_data 중 하나만 사용해야 합니다.
        headers: 요청 헤더 딕셔너리 (예: {"Authorization": "Bearer token"})
        timeout: 요청 타임아웃 시간 (초 단위, 기본값 10초)
        verify_ssl: SSL 인증서 검증 여부 (기본값 True)
    
    Returns:
        requests.Response: 응답 객체
    
    Raises:
        HttpRequestError: 요청 실패 시
    
    Examples:
        >>> response = post_request(
        ...     "http://example.com/api/users",
        ...     json_data={"username": "john", "email": "john@example.com"}
        ... )
        >>> response.json()
        
        >>> response = post_request(
        ...     "http://example.com/api/users",
        ...     data={"username": "john", "email": "john@example.com"},
        ...     headers={"Content-Type": "application/x-www-form-urlencoded"}
        ... )
        
        >>> response = post_request(
        ...     "http://example.com/api/users",
        ...     json_data={"user_id": 123, "status": "active"},
        ...     headers={"Authorization": "Bearer token123"}
        ... )
    """
    try:
        logger.info(f"POST 요청 시작: {url}, data/json_data 포함")
        
        request_kwargs = {
            "timeout": timeout,
            "verify": verify_ssl
        }
        
        if data is not None:
            request_kwargs["data"] = data
        if json_data is not None:
            request_kwargs["json"] = json_data
        if headers is not None:
            request_kwargs["headers"] = headers
        
        response = requests.post(url, **request_kwargs)
        response.raise_for_status()
        logger.info(f"POST 요청 성공: {url}, status_code={response.status_code}")
        return response
    except requests.exceptions.Timeout:
        error_msg = f"POST 요청 타임아웃: {url} (timeout={timeout}초)"
        logger.error(error_msg)
        raise HttpRequestError(error_msg)
    except requests.exceptions.ConnectionError as e:
        error_msg = f"POST 요청 연결 실패: {url}, error={str(e)}"
        logger.error(error_msg)
        raise HttpRequestError(error_msg)
    except requests.exceptions.HTTPError as e:
        error_msg = f"POST 요청 HTTP 오류: {url}, status_code={response.status_code}, error={str(e)}"
        logger.error(error_msg)
        raise HttpRequestError(error_msg)
    except Exception as e:
        error_msg = f"POST 요청 실패: {url}, error={str(e)}"
        logger.error(error_msg)
        raise HttpRequestError(error_msg)


def get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
    verify_ssl: bool = True
) -> Dict[str, Any]:
    """
    GET 요청을 보내고 JSON 응답을 반환합니다.
    
    Args:
        url: 요청할 URL
        params: URL 쿼리 파라미터
        headers: 요청 헤더
        timeout: 요청 타임아웃 시간
        verify_ssl: SSL 인증서 검증 여부
    
    Returns:
        Dict[str, Any]: 파싱된 JSON 응답
    
    Examples:
        >>> data = get_json("http://example.com/api/users", params={"page": 1})
        >>> print(data["users"])
    """
    response = get_request(url, params, headers, timeout, verify_ssl)
    return response.json()


def post_json(
    url: str,
    json_data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
    verify_ssl: bool = True
) -> Dict[str, Any]:
    """
    POST 요청을 보내고 JSON 응답을 반환합니다.
    
    Args:
        url: 요청할 URL
        json_data: 요청 본문 (JSON 형식)
        headers: 요청 헤더
        timeout: 요청 타임아웃 시간
        verify_ssl: SSL 인증서 검증 여부
    
    Returns:
        Dict[str, Any]: 파싱된 JSON 응답
    
    Examples:
        >>> data = post_json(
        ...     "http://example.com/api/users",
        ...     json_data={"username": "john", "email": "john@example.com"}
        ... )
        >>> print(data["id"])
    """
    response = post_request(url, json_data=json_data, headers=headers, timeout=timeout, verify_ssl=verify_ssl)
    return response.json()


def post_form_data(
    url: str,
    data: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
    verify_ssl: bool = True
) -> requests.Response:
    """
    폼 데이터 형식의 POST 요청을 보냅니다.
    
    Args:
        url: 요청할 URL
        data: 폼 데이터 딕셔너리
        headers: 요청 헤더
        timeout: 요청 타임아웃 시간
        verify_ssl: SSL 인증서 검증 여부
    
    Returns:
        requests.Response: 응답 객체
    
    Examples:
        >>> response = post_form_data(
        ...     "http://example.com/api/users",
        ...     data={"username": "john", "password": "secret123"}
        ... )
    """
    return post_request(url, data=data, headers=headers, timeout=timeout, verify_ssl=verify_ssl)


def request_with_retry(
    method: str,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Union[Dict[str, Any], str]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
    verify_ssl: bool = True,
    max_retries: int = 3,
    backoff_factor: float = 0.5
) -> requests.Response:
    """
    재시도 로직이 포함된 HTTP 요청을 보냅니다.
    
    Args:
        method: HTTP 메서드 ("GET" 또는 "POST")
        url: 요청할 URL
        params: URL 쿼리 파라미터
        data: 폼 데이터
        json_data: JSON 데이터
        headers: 요청 헤더
        timeout: 요청 타임아웃 시간
        verify_ssl: SSL 인증서 검증 여부
        max_retries: 최대 재시도 횟수 (기본값 3)
        backoff_factor: 재시도 간 대기 시간 계산 계수 (기본값 0.5)
    
    Returns:
        requests.Response: 응답 객체
    
    Raises:
        HttpRequestError: 모든 재시도 실패 시
    
    Examples:
        >>> response = request_with_retry(
        ...     "GET",
        ...     "http://example.com/api/users",
        ...     params={"page": 1},
        ...     max_retries=3
        ... )
    """
    import time
    
    method = method.upper()
    for attempt in range(max_retries):
        try:
            if method == "GET":
                return get_request(url, params, headers, timeout, verify_ssl)
            elif method == "POST":
                return post_request(url, data, json_data, headers, timeout, verify_ssl)
            else:
                raise HttpRequestError(f"지원하지 않는 HTTP 메서드: {method}")
        except HttpRequestError as e:
            if attempt < max_retries - 1:
                wait_time = backoff_factor * (2 ** attempt)
                logger.warning(f"{method} 요청 실패 (시도 {attempt + 1}/{max_retries}), {wait_time}초 후 재시도...")
                time.sleep(wait_time)
            else:
                logger.error(f"{method} 요청 최대 재시도 횟수 초과")
                raise