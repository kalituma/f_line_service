import json
import os
import logging
from typing import Optional, Dict, Any, List, AnyStr

logger = logging.getLogger(__name__)

def validate_config(config_data: Dict[str, Any], required_fields: List[AnyStr]) -> bool:
    """
    설정 데이터의 필수 필드를 검증합니다.

    Args:
        config_data: 설정 데이터

    Returns:
        검증 성공 여부
    """

    for field in required_fields:
        if field not in config_data:
            logger.error(f"Missing required field: {field}")
            return False

    if not isinstance(config_data["video_info"], list):
        logger.error("video_info must be a list")
        return False

    if len(config_data["video_info"]) == 0:
        logger.warning("video_info list is empty")
        return False

    for idx, video_item in enumerate(config_data["video_info"]):
        if "video_type" not in video_item or "video_path" not in video_item:
            logger.error(
                f"video_info[{idx}] missing required fields "
                f"(video_type, video_path)"
            )
            return False

    logger.info("Config validation passed")
    return True

def load_config_file(config_file_path: str) -> Optional[Dict[str, Any]]:
    """
    JSON 설정 파일을 읽습니다.

    Args:
        config_file_path: 설정 파일 경로

    Returns:
        파싱된 설정 데이터 또는 None
    """
    try:
        if not os.path.exists(config_file_path):
            logger.error(f"Config file not found: {config_file_path}")
            return None

        with open(config_file_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)

        logger.info(f"Successfully loaded config file: {config_file_path}")
        return config_data

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON file: {e}")
        return None
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        return None