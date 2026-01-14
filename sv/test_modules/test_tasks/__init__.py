from typing import Dict, Any, List

def split_primary_task_result(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [video for video in result['videos'] if video.get('analysis_status') == "STAT_001"]