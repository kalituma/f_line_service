from typing import List, Dict, Any

def result_without_id(result: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results_without_id = [{k: v for k, v in r.items() if k != 'doc_id'} for r in result]
    return results_without_id