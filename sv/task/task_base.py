import logging
from typing import Dict, Any

from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class TaskBase:
    """작업의 기본 클래스"""

    def __init__(self, task_name: str):
        self.task_name = task_name
        self.logger = logging.getLogger(f"Task-{task_name}")

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """작업 실행 (서브클래스에서 구현)

        Args:
            context: 이전 작업들의 결과를 담은 컨텍스트

        Returns:
            작업 결과 딕셔너리
        """
        raise NotImplementedError