# JobQueueService 사용 가이드

`JobQueueService`는 `sv/backend/db/job_queue.py`의 `JobQueue` 클래스를 활용하여 작업 큐와 태스크를 관리하는 서비스입니다.

## 📋 개요

- **파일**: `job_queue_service.py`
- **주요 클래스**: `JobQueueService`
- **의존성**: `sv.backend.db.job_queue.JobQueue`, `JobStatus`

## 🚀 빠른 시작

### 기본 사용법

```python
from server.backend.service.job_queue_service import JobQueueService
from sv.backend.db.job_queue import JobStatus

# 서비스 초기화
service = JobQueueService(db_path="jobs.db")

# 작업 추가
job_id = service.add_job("FIRE_001", "ANALYSIS_001")

# 작업 상태 조회
job = service.get_job_by_id(job_id)

# 다음 처리할 작업 가져오기
next_job_id = service.get_next_job()
```

## 📚 API 문서

### 1. **add_job(frfr_id, analysis_id, status)**
새 작업을 큐에 추가합니다.

**파라미터:**
- `frfr_id` (str): 산불 정보 ID
- `analysis_id` (str): 분석 ID
- `status` (JobStatus): 초기 상태 (기본값: PENDING)

**반환값:** `int` (job_id) 또는 `None` (중복인 경우)

**예제:**
```python
job_id = service.add_job("FIRE_20230101_SEOUL", "ANALYSIS_001")
```

---

### 2. **get_next_job()**
FIFO 순서로 다음 pending 상태의 작업을 가져오고 processing으로 변경합니다.

**반환값:** `int` (job_id) 또는 `None` (pending 작업이 없는 경우)

**예제:**
```python
job_id = service.get_next_job()
if job_id:
    print(f"처리할 작업: {job_id}")
```

---

### 3. **initialize_tasks(job_id, task_names)**
작업에 대한 태스크를 초기화합니다.

**파라미터:**
- `job_id` (int): 작업 ID
- `task_names` (List[str]): 태스크 이름 리스트

**반환값:** `bool` (성공 여부)

**예제:**
```python
success = service.initialize_tasks(1, ["extract", "analyze", "save"])
```

---

### 4. **add_job_with_tasks(frfr_id, analysis_id, task_names)**
작업을 추가하고 바로 태스크를 초기화하는 편의 메서드입니다.

**파라미터:**
- `frfr_id` (str): 산불 정보 ID
- `analysis_id` (str): 분석 ID
- `task_names` (List[str]): 태스크 이름 리스트

**반환값:** `int` (job_id) 또는 `None` (실패한 경우)

**예제:**
```python
job_id = service.add_job_with_tasks(
    "FIRE_001",
    "ANALYSIS_001",
    ["extract", "analyze", "save"]
)
```

---

### 5. **get_job_by_id(job_id)**
작업 ID로 작업 정보를 조회합니다.

**파라미터:**
- `job_id` (int): 작업 ID

**반환값:** `Dict` 또는 `None` (작업이 없는 경우)

**예제:**
```python
job = service.get_job_by_id(1)
print(job)
# {'job_id': 1, 'frfr_id': 'FIRE_001', 'analysis_id': 'ANALYSIS_001', 'status': 'pending', ...}
```

---

### 6. **get_jobs_by_status(status)**
특정 상태의 모든 작업을 조회합니다.

**파라미터:**
- `status` (JobStatus): 조회할 작업 상태

**반환값:** `List[Dict]` (작업 정보 리스트)

**예제:**
```python
from sv.backend.db.job_queue import JobStatus

# Pending 상태의 모든 작업 조회
pending_jobs = service.get_jobs_by_status(JobStatus.PENDING)

# Processing 상태의 모든 작업 조회
processing_jobs = service.get_jobs_by_status(JobStatus.PROCESSING)

# 완료된 모든 작업 조회
completed_jobs = service.get_jobs_by_status(JobStatus.COMPLETED)
```

---

### 7. **update_job_status(job_id, status)**
작업의 상태를 업데이트합니다.

**파라미터:**
- `job_id` (int): 작업 ID
- `status` (JobStatus): 변경할 상태

**반환값:** `bool` (성공 여부)

**예제:**
```python
service.update_job_status(1, JobStatus.COMPLETED)
```

---

### 8. **get_job_tasks(job_id)**
작업의 모든 태스크를 조회합니다.

**파라미터:**
- `job_id` (int): 작업 ID

**반환값:** `List[Dict]` (태스크 정보 리스트)

**예제:**
```python
tasks = service.get_job_tasks(1)
for task in tasks:
    print(f"{task['task_name']}: {task['status']}")
```

---

### 9. **update_task_status(task_id, status)**
태스크의 상태를 업데이트합니다.

**파라미터:**
- `task_id` (int): 태스크 ID
- `status` (JobStatus): 변경할 상태

**반환값:** `bool` (성공 여부)

**예제:**
```python
service.update_task_status(1, JobStatus.PROCESSING)
```

---

### 10. **delete_job(job_id)**
작업을 삭제합니다 (관련된 태스크도 함께 삭제됨).

**파라미터:**
- `job_id` (int): 작업 ID

**반환값:** `bool` (성공 여부)

**예제:**
```python
service.delete_job(1)
```

---

### 11. **get_all_jobs()**
모든 작업을 조회합니다.

**반환값:** `List[Dict]` (모든 작업 정보 리스트)

**예제:**
```python
all_jobs = service.get_all_jobs()
print(f"전체 작업 개수: {len(all_jobs)}")
```

---

## 🔄 작업 상태 (JobStatus)

```python
from sv.backend.db.job_queue import JobStatus

JobStatus.PENDING     # 대기 중
JobStatus.PROCESSING  # 처리 중
JobStatus.COMPLETED   # 완료
JobStatus.FAILED      # 실패
JobStatus.CANCELLED   # 취소됨
```

## 💡 사용 예제

### 예제 1: 기본 작업 처리 흐름

```python
from server.backend.service.job_queue_service import JobQueueService
from sv.backend.db.job_queue import JobStatus

service = JobQueueService()

# 1. 새 작업 추가
job_id = service.add_job("FIRE_001", "ANALYSIS_001")
print(f"작업 추가됨: {job_id}")

# 2. 다음 처리할 작업 가져오기
next_job_id = service.get_next_job()
print(f"처리할 작업: {next_job_id}")

# 3. 작업 상태 확인
job = service.get_job_by_id(next_job_id)
print(f"상태: {job['status']}")

# 4. 작업 완료
service.update_job_status(next_job_id, JobStatus.COMPLETED)
print("작업 완료됨")
```

### 예제 2: 작업과 태스크 함께 관리

```python
# 작업과 태스크 함께 생성
job_id = service.add_job_with_tasks(
    "FIRE_001",
    "ANALYSIS_001",
    ["video_extract", "frame_analysis", "result_save"]
)

# 태스크 처리
tasks = service.get_job_tasks(job_id)
for task in tasks:
    # 태스크 처리 중으로 표시
    service.update_task_status(task['task_id'], JobStatus.PROCESSING)
    
    # 태스크 처리 로직...
    
    # 태스크 완료로 표시
    service.update_task_status(task['task_id'], JobStatus.COMPLETED)

# 모든 태스크 완료 후 작업 완료
service.update_job_status(job_id, JobStatus.COMPLETED)
```

### 예제 3: 상태별 작업 조회 및 처리

```python
# Pending 상태의 모든 작업 조회
pending_jobs = service.get_jobs_by_status(JobStatus.PENDING)

for job in pending_jobs:
    job_id = job['job_id']
    
    # 작업 처리
    service.update_job_status(job_id, JobStatus.PROCESSING)
    
    # ... 처리 로직 ...
    
    service.update_job_status(job_id, JobStatus.COMPLETED)
```

## 🔧 설정

### 데이터베이스 경로 변경

```python
# 기본값: "jobs.db"
service = JobQueueService(db_path="/path/to/custom_jobs.db")
```

### 로깅 설정

```python
import logging

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
```

## ⚠️ 주의사항

1. **중복 작업**: 같은 `frfr_id`와 `analysis_id` 조합의 작업은 중복 추가되지 않습니다.
2. **동시성**: SQLite를 사용하므로 고도의 동시성이 필요한 경우 다른 데이터베이스 사용 고려
3. **트랜잭션**: 모든 작업은 자동으로 트랜잭션으로 처리됩니다.

## 📝 데이터베이스 스키마

### job_queue 테이블

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| job_id | INTEGER | 작업 ID (Primary Key) |
| frfr_id | TEXT | 산불 정보 ID |
| analysis_id | TEXT | 분석 ID |
| status | TEXT | 작업 상태 |
| created_at | REAL | 생성/수정 시간 |

### tasks 테이블

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| task_id | INTEGER | 태스크 ID (Primary Key) |
| job_id | INTEGER | 작업 ID (Foreign Key) |
| task_name | TEXT | 태스크 이름 |
| status | TEXT | 태스크 상태 |
| seq | INTEGER | 태스크 순서 |
| updated_at | REAL | 업데이트 시간 |

## 🔗 관련 파일

- `sv/backend/db/job_queue.py` - JobQueue 클래스 (기본 구현)
- `job_queue_service.py` - JobQueueService 클래스
- `job_queue_service_example.py` - 사용 예제

## ✅ 테스트

예제 파일을 실행하여 기능을 테스트할 수 있습니다:

```bash
python server/backend/service/job_queue_service_example.py
```

