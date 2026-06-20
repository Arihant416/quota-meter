from pydantic import BaseModel, Field
from datetime import datetime


class ConsumeRequest(BaseModel):
    org_id: str
    feature: str
    units: int = Field(..., gt=0)
    idempotency_key: str


class QuotaResult(BaseModel):
    granted: bool
    remaining: int = Field(..., ge=0)
    resets_at: datetime
    org_id: str
    feature: str


class UsageResponse(BaseModel):
    org_id: str
    feature: str
    period: str
    limit: int
    used: int
    remaining: int = Field(..., ge=0)
    resets_at: datetime


class RefundRequest(BaseModel):
    org_id: str
    feature: str
    units: int = Field(..., gt=0)
    idempotency_key: str