"""
동일한 analysis_id를 중복 전송하지 않도록 로컬에 기록한다.
"""
import json
import os
from datetime import datetime, timezone
from typing import Dict, Optional

from loguru import logger

StateDict = Dict[str, dict]


class StateStore:
    def __init__(self, path: str):
        """상태 파일 경로를 설정하고 기존 상태를 불러온다."""
        self.path = path
        self.state: StateDict = {"sent": {}, "last_dispatch_at": None}
        self._load()

    def _load(self):
        """상태 파일에서 보낸 항목과 마지막 전송 시각을 읽어온다."""
        dir_path = os.path.dirname(self.path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        if not os.path.exists(self.path):
            self._save()
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
                self.state["sent"] = raw.get("sent", {})
                self.state["last_dispatch_at"] = raw.get("last_dispatch_at")
        except Exception as exc:
            logger.error(f"상태 파일 {self.path} 로드 실패: {exc}")
            self.state = {"sent": {}, "last_dispatch_at": None}
            self._save()

    def _save(self):
        """현재 상태를 JSON 파일로 저장한다."""
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=True, indent=2)
        except Exception as exc:
            logger.error(f"상태 파일 {self.path} 저장 실패: {exc}")

    def is_sent(self, analysis_id: str) -> bool:
        """해당 analysis_id가 이미 전송되었는지 확인한다."""
        return analysis_id in self.state["sent"]

    def mark_sent(self, analysis_id: str, event_cd: str):
        """전송된 analysis_id와 event_cd를 기록하고 마지막 전송 시각을 갱신한다."""
        now = datetime.now(timezone.utc).isoformat()
        self.state["sent"][analysis_id] = {"event_cd": event_cd, "sent_at": now}
        self.state["last_dispatch_at"] = now
        self._save()

    def bump_last_dispatch(self):
        """마지막 전송 시각만 현재로 갱신한다."""
        self.state["last_dispatch_at"] = datetime.now(timezone.utc).isoformat()
        self._save()

    def remove(self, analysis_id: str):
        """특정 analysis_id의 전송 기록을 제거한다."""
        if analysis_id in self.state["sent"]:
            self.state["sent"].pop(analysis_id, None)
            self._save()

    def prune_completed(self, completed_ids):
        """완료된 analysis_id 목록을 받아 로컬 상태에서 일괄 제거한다."""
        removed = 0
        for analysis_id in list(self.state["sent"].keys()):
            if analysis_id in completed_ids:
                self.state["sent"].pop(analysis_id, None)
                removed += 1
        if removed:
            self._save()
        return removed

    def last_dispatch_at(self) -> Optional[datetime]:
        """마지막 전송 시각을 datetime 형태로 반환한다."""
        ts = self.state.get("last_dispatch_at")
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts)
        except ValueError:
            return None

