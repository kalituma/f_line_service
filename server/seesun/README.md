# Wildfire Analysis Dispatcher (Python)

AI 모델로 보낼 `heli.analysis_info` 분석 그룹을 25분 간격으로 순차 전달하는 파이썬 서비스입니다. `analysis_status`가 `GROUP_001`인 레코드 중 가장 오래된 것부터 `event_cd`, `analysis_id`를 전송하며, 동일 분석그룹은 `GROUP_003` 또는 `GROUP_004`로 상태가 바뀌기 전까지 다시 보내지 않습니다.

## 주요 동작
- DB 조회: `heli.analysis_info`에서 `GROUP_001`을 생성일 오름차순으로 조회
- DB 업데이트: 전송 성공 시 선택적으로 `analysis_status`를 `GROUP_002`로 업데이트(`mark_group002_on_dispatch=True`).
- 중복 방지: 로컬 상태 파일(`state/dispatched.json`)로 이미 전송한 `analysis_id` 관리
- 완료 처리: 전송된 항목의 상태가 `GROUP_003`/`GROUP_004`가 되면 로컬 상태에서 제거
- 간격 제어: 마지막 전송 시점으로부터 25분이 지나야 다음 건을 전송
- 전송 방식: 기본은 AI 내부 함수 호출(`ai_runner.start_analysis`)이며, 필요 시 `send_func`로 HTTP 전송 함수로 교체 가능. HTTP를 쓰지 않으면 `ai_dispatch_url`은 비워둔다.

## 구성 파일
- `config.py` : 환경설정(환경변수/.env로 오버라이드 가능)
- `database.py` : Postgres 연결 및 조회/업데이트 헬퍼
- `state_store.py` : 로컬 중복 방지 상태 관리
- `dispatcher.py` : 전송 로직과 스케줄 루프
- `main.py` : 서비스 진입점
- `requirements.txt` : 의존성 목록

## 설치 및 실행
```bash
python -m venv .venv
.venv\\Scripts\\activate  # Windows
pip install -r wildfire-analysis-dispatcher/requirements.txt
python wildfire-analysis-dispatcher/main.py
```

## 기본: AI 내부 함수로 직접 호출
`ai_runner.start_analysis(event_cd, analysis_cd)`가 기본 전송 경로입니다. 실제 AI 분석 시작 로직을 `ai_runner.py`에서 구현하세요.

```python
## HTTP로 보내고 싶을 때
HTTP 엔드포인트로 전송하려면 `AnalysisDispatcher(send_func=dispatcher.AnalysisDispatcher._send_to_ai_http)` 형태로 전달하거나, 커스텀 `send_func`를 등록하고 `.env`에 `AI_DISPATCH_URL`을 설정하세요.
```

## Docker Compose로 실행
루트의 `docker-compose.yml`에 `wildfire-analysis-dispatcher` 서비스가 추가되어 있습니다. AI 모델 URL/토큰만 `.env` 또는 compose 환경변수로 맞춘 뒤 실행하세요.
```bash
docker-compose up -d wildfire-analysis-dispatcher
```

## 환경 변수 (.env 예시, HTTP 사용 시)
```
DB_HOST=wildfire-postgis
DB_PORT=5432
DB_NAME=geoserver
DB_USER=post132gres
DB_PASSWORD=se7cret!8A5c
AI_DISPATCH_URL=http://ai-model.example.com/api/analysis/start
AI_AUTH_TOKEN=
POLL_INTERVAL_SECONDS=30
DISPATCH_INTERVAL_MINUTES=25
REQUEST_TIMEOUT_SECONDS=10
MARK_GROUP002_ON_DISPATCH=true
STATE_FILE=./wildfire-analysis-dispatcher/state/dispatched.json
LOG_LEVEL=INFO
BACKOFF_ON_FAILURE=true
```

## 전달 페이로드
```json
{ "frfr_info_id": "<산불번호>", "analysis_id": "<분석그룹ID>" }
```

## 재시작 시 중복 방지
전송 기록은 `STATE_FILE` 경로에 JSON으로 저장됩니다. 서비스 재시작 후에도 이미 보낸 `analysis_id`는 상태가 `GROUP_003` 또는 `GROUP_004`가 될 때까지 다시 보내지 않습니다.

