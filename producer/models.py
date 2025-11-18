from pydantic import BaseModel
from typing import Optional, Any
from uuid import UUID
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "pending"
    CAPTURING = "capturing"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"

class ScreenshotRequest(BaseModel):
    url: str
    selector: Optional[str] = None

class ScreenshotTask(BaseModel):
    job_id: UUID
    url: str
    selector: Optional[str] = None

class StorageTask(BaseModel):
    job_id: UUID
    base64_image: str

class StatusUpdate(BaseModel):
    job_id: UUID
    status: JobStatus
    message: Optional[str] = None
    image_url: Optional[str] = None

class DefaultResponse(BaseModel):
    error: bool
    message: str
    payload: Optional[Any] = None