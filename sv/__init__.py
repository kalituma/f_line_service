from pathlib import Path

# ==================== 프로젝트 경로 상수 ====================

# sv/ 디렉토리 경로
SV_DIR_PATH = Path(__file__).parent.resolve()

# 프로젝트 루트 경로 (sv/의 부모 디렉토리)
PROJECT_ROOT_PATH = SV_DIR_PATH.parent.resolve()

# 데이터 디렉토리 경로
DATA_DIR_PATH = PROJECT_ROOT_PATH / "data"


# 데이터베이스 디렉토리 경로
SQLITE_DIR_PATH = DATA_DIR_PATH / "sqlite"
SQLITE_DIR_PATH.mkdir(parents=True, exist_ok=True)

# 작업 큐 DB 파일 경로
JOB_QUEUE_DB_PATH = SQLITE_DIR_PATH / "jobs.db"

# 로그 디렉토리 경로
LOG_DIR_PATH = PROJECT_ROOT_PATH / "logs"
LOG_DIR_PATH.mkdir(parents=True, exist_ok=True)

# 공통 로그 파일 경로 (서버 시작 시 설정됨)
COMMON_LOG_FILE = None

# ==================== 공통 설정 ====================

# 기본 데이터베이스 경로 (문자열 형식)
DEFAULT_JOB_QUEUE_DB = str(JOB_QUEUE_DB_PATH)

__all__ = [
    'SV_DIR_PATH',
    'PROJECT_ROOT_PATH',
    'DATA_DIR_PATH',
    'SQLITE_DIR_PATH',
    'JOB_QUEUE_DB_PATH',
    'LOG_DIR_PATH',
    'DEFAULT_JOB_QUEUE_DB',
    'COMMON_LOG_FILE',
]

