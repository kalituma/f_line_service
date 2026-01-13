from enum import Enum

class JobStatus(Enum):
    """작업 상태 Enum"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    def __str__(self):
        return self.value

