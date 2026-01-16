from datetime import datetime
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

def _format_timestamps(row_dict: dict) -> dict:
    """
    row 데이터의 created_at과 updated_at을 readable 날짜 형식으로 변환 (ISO 8601)

    Args:
        row_dict: row 데이터 dict

    Returns:
        포맷된 dict (created_at, updated_at: 'YYYY-MM-DDTHH:MM:SS.mmmmmm')
    """
    if 'created_at' in row_dict and row_dict['created_at']:
        try:
            row_dict['created_at'] = datetime.fromtimestamp(row_dict['created_at']).isoformat()
        except (ValueError, OSError, TypeError):
            logger.warning(f"Invalid created_at timestamp: {row_dict['created_at']}")

    if 'updated_at' in row_dict and row_dict['updated_at']:
        try:
            row_dict['updated_at'] = datetime.fromtimestamp(row_dict['updated_at']).isoformat()
        except (ValueError, OSError, TypeError):
            logger.warning(f"Invalid updated_at timestamp: {row_dict['updated_at']}")

    return row_dict